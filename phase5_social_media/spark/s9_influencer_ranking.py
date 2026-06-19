# Step 9: Influencer Ranking
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, sum as spark_sum
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import datetime
import snowflake.connector
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema

WEIGHTS = {
    'LIKE': 1,
    'COMMENT': 3,
    'SHARE': 5,
    'FOLLOW': 10,
}

BASE_DIR = os.path.dirname(__file__)
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

stream_df = raw_stream.select(
    from_json(col('value').cast('string'), schema).alias('d')
).select('d.*')

weighted_df = stream_df.filter(
    col('event_type').isin(list(WEIGHTS.keys()))
).withColumn(
    'engagement_score',
    when(col('event_type') == 'LIKE',    lit(WEIGHTS['LIKE']))
    .when(col('event_type') == 'COMMENT', lit(WEIGHTS['COMMENT']))
    .when(col('event_type') == 'SHARE',   lit(WEIGHTS['SHARE']))
    .when(col('event_type') == 'FOLLOW',  lit(WEIGHTS['FOLLOW']))
    .otherwise(lit(0))
)

agg_df = (
    weighted_df
    .filter(col('target_user_id').isNotNull())
    .groupBy(col('target_user_id').alias('user_id'))
    .agg(spark_sum('engagement_score').alias('total_engagement'))
)


def rank_and_write(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    ranked = batch_df.withColumn(
        'rank',
        F.rank().over(Window.orderBy(col('total_engagement').desc())),
    )

    pdf = ranked.toPandas()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    for _, row in pdf.iterrows():
        cur.execute(
            'INSERT INTO ANALYTICS.INFLUENCER_RANKINGS '
            '(user_id, total_engagement, rank, calculated_at) '
            'VALUES (%s, %s, %s, %s)',
            (
                str(row['user_id']),
                int(row['total_engagement']),
                int(row['rank']),
                now_str,
            ),
        )

    conn.close()
    print(f'  Batch {batch_id}: {len(pdf)} influencer ranks → INFLUENCER_RANKINGS')


query = agg_df.writeStream \
    .outputMode('complete') \
    .foreachBatch(rank_and_write) \
    .trigger(processingTime='30 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Influencer ranking stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
