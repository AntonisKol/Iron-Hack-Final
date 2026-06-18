from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, window, count, current_timestamp, lit, from_json
)
from datetime import datetime
import snowflake.connector
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema

BASE_DIR   = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's7_checkpoint')

spark = SparkSession.builder \
    .appName('S7 - Trending Hashtags') \
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
    .withWatermark('ts', '5 minutes')
)

hashtag_df = (
    stream_df
    .filter(col('hashtags').isNotNull())
    .select(col('ts'), explode(col('hashtags')).alias('hashtag'))
)


def make_window_agg(df, window_duration, slide_duration=None):
    w = window(col('ts'), window_duration, slide_duration) if slide_duration \
        else window(col('ts'), window_duration)
    return (
        df.groupBy(w, col('hashtag'))
          .agg(count('*').alias('mention_count'))
          .withColumn('window_size', lit(window_duration))
    )

agg_1m  = make_window_agg(hashtag_df, '1 minute')
agg_5m  = make_window_agg(hashtag_df, '5 minutes')
agg_15m = make_window_agg(hashtag_df, '15 minutes')

combined = agg_1m.union(agg_5m).union(agg_15m)


def write_trending(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf     = batch_df.toPandas()
    conn    = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur     = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    rows    = []

    for _, row in pdf.iterrows():
        rows.append((
            str(row['hashtag']),
            str(row['window_size']),
            str(row['window']['start']),
            str(row['window']['end']),
            int(row['mention_count']),
            now_str,
        ))

    cur.executemany(
        'INSERT INTO ANALYTICS.TRENDING_HASHTAGS '
        '(hashtag, window_size, window_start, window_end, mention_count, calculated_at) '
        'VALUES (%s, %s, %s, %s, %s, %s)',
        rows,
    )
    conn.close()
    print(f'  Batch {batch_id}: {len(rows)} hashtag-window rows → TRENDING_HASHTAGS')


query = combined.writeStream \
    .outputMode('update') \
    .foreachBatch(write_trending) \
    .trigger(processingTime='30 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Trending hashtags stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
