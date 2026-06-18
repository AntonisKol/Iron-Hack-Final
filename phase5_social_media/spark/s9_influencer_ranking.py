# Step 9: Influencer Ranking
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum as _sum, when, lit, from_json
)
from datetime import datetime
import snowflake.connector
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema

BASE_DIR   = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's9_checkpoint')

spark = SparkSession.builder \
    .appName('S9 - Influencer Ranking') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
_log4j = spark.sparkContext._jvm.org.apache.log4j
_log4j.Logger.getLogger('org.apache.spark.sql.kafka010.KafkaDataConsumer').setLevel(_log4j.Level.ERROR)

raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'social-events') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

stream_df = (
    raw_stream
    .select(from_json(col('value').cast('string'), schema).alias('d'))
    .select('d.*')
    .withColumn('ts', col('timestamp').cast('timestamp'))
    .withWatermark('ts', '10 minutes')
    .withColumn(
        'weight',
        when(col('event_type') == 'LIKE',        lit(1.0))
        .when(col('event_type') == 'COMMENT',     lit(3.0))
        .when(col('event_type') == 'SHARE',       lit(5.0))
        .when(col('event_type') == 'VIDEO_VIEW',  lit(0.5))
        .when(col('event_type') == 'FOLLOW',      lit(2.0))
        .otherwise(lit(0.0))
    )
)

engagement_agg = (
    stream_df
    .groupBy(window(col('ts'), '15 minutes'), col('user_id'))
    .agg(
        _sum('weight').alias('engagement_score'),
        count(when(col('event_type') == 'LIKE',       True)).alias('like_count'),
        count(when(col('event_type') == 'COMMENT',    True)).alias('comment_count'),
        count(when(col('event_type') == 'SHARE',      True)).alias('share_count'),
        count(when(col('event_type') == 'VIDEO_VIEW', True)).alias('video_view_count'),
        count(when(col('event_type') == 'FOLLOW',     True)).alias('follow_count'),
    )
)


def write_rankings(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf     = batch_df.toPandas()
    conn    = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur     = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    rows    = []

    pdf['rank'] = pdf['engagement_score'].rank(ascending=False, method='min').astype(int)

    for _, row in pdf.iterrows():
        rows.append((
            str(row['user_id']),
            str(row['window']['start']),
            str(row['window']['end']),
            float(row['engagement_score']),
            int(row['like_count']),
            int(row['comment_count']),
            int(row['share_count']),
            int(row['video_view_count']),
            int(row['follow_count']),
            int(row['rank']),
            now_str,
        ))

    cur.executemany(
        'INSERT INTO ANALYTICS.INFLUENCER_RANKING ('
        '  user_id, window_start, window_end, engagement_score,'
        '  like_count, comment_count, share_count, video_view_count, follow_count,'
        '  rank, calculated_at'
        ') VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        rows,
    )
    conn.close()

    top = pdf.nlargest(5, 'engagement_score')[['user_id', 'engagement_score']]
    print(f'  Batch {batch_id}: {len(rows)} user-window rows → INFLUENCER_RANKING')
    print(f'  Top 5 this batch:\n{top.to_string(index=False)}\n')


query = engagement_agg.writeStream \
    .outputMode('update') \
    .foreachBatch(write_rankings) \
    .trigger(processingTime='30 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Influencer ranking stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
