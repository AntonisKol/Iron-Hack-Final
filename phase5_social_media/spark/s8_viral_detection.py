# Step 8: Viral Content Detection
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window, from_json
from datetime import datetime
import snowflake.connector
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema

VIRAL_THRESHOLD = 50

BASE_DIR = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's8_checkpoint')

spark = SparkSession.builder \
    .appName('S8 - Viral Detection') \
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

like_counts = (
    stream_df
    .filter(col('event_type') == 'LIKE')
    .filter(col('post_id').isNotNull())
    .groupBy(window(col('ts'), '5 minutes'), col('post_id'))
    .agg(count('*').alias('like_count'))
)


def detect_viral(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    viral_df = batch_df.filter(col('like_count') >= VIRAL_THRESHOLD)

    if viral_df.isEmpty():
        return

    pdf = viral_df.toPandas()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    for _, row in pdf.iterrows():
        cur.execute(
            'INSERT INTO ANALYTICS.VIRAL_POSTS '
            '(post_id, window_start, window_end, like_count, detected_at) '
            'VALUES (%s, %s, %s, %s, %s)',
            (
                str(row['post_id']),
                str(row['window']['start']),
                str(row['window']['end']),
                int(row['like_count']),
                now_str,
            ),
        )

    conn.close()
    print(f'  Batch {batch_id}: {len(pdf)} viral posts detected → VIRAL_POSTS')


query = like_counts.writeStream \
    .outputMode('update') \
    .foreachBatch(detect_viral) \
    .trigger(processingTime='30 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Viral detection stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
