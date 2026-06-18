from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.types import StructType, StringType
import os

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('Q14 - Page View Stream') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── SCHEMA ────────────────────────────────────────────────────────────────────
# Explicit schema for from_json — Spark cannot infer from a streaming source.
# timestamp is parsed as StringType then cast to timestamp so withWatermark works.
schema = StructType() \
    .add('user_id',   StringType()) \
    .add('page',      StringType()) \
    .add('timestamp', StringType())

# ── READ FROM KAFKA ───────────────────────────────────────────────────────────
# Each Kafka message value is JSON bytes → cast to string → parse with schema.
# 'page-views' is the topic the producer writes to.
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'page-views') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

stream_df = (
    raw_stream
    .select(from_json(col('value').cast('string'), schema).alias('d'))
    .select('d.*')
    .withColumn('ts', col('timestamp').cast('timestamp'))
)

# ── WATERMARK + TUMBLING WINDOW ───────────────────────────────────────────────
# withWatermark("ts", "10 minutes"):
#   Spark accepts late events up to 10 minutes behind the max seen timestamp.
#   Events older than the watermark line are dropped — this bounds state size.
#
# window(col("ts"), "5 minutes"):
#   Groups events into 5-minute tumbling buckets based on event timestamp.
#   Tumbling = non-overlapping: each event belongs to exactly one bucket.
#
# groupBy(window, page) + count():
#   Within each 5-minute bucket, count views per page.
windowed_counts = (
    stream_df
    .withWatermark('ts', '10 minutes')
    .groupBy(
        window(col('ts'), '5 minutes'),
        col('page')
    )
    .count()
)

# ── WRITE TO CONSOLE ──────────────────────────────────────────────────────────
# outputMode='update' — print only rows that changed in this micro-batch.
# trigger='10 seconds' — process new Kafka messages every 10 seconds.
# truncate=False — show full window timestamps without cutting them off.
query = windowed_counts.writeStream \
    .outputMode('update') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime='10 seconds') \
    .start()

print('\nStreaming started. Reading from Kafka topic: page-views')
print('Run q14_pageview_producer.py in another terminal.\n')

query.awaitTermination()
