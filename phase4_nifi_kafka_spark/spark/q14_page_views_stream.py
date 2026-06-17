from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StructType, StringType, TimestampType
import os

INPUT_DIR = os.path.join(os.path.dirname(__file__), 'pageviews_input')
os.makedirs(INPUT_DIR, exist_ok=True)

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('Q14 - Page View Stream') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── SCHEMA ────────────────────────────────────────────────────────────────────
# Explicit schema is required for readStream — Spark cannot infer schema
# from a streaming source because it hasn't seen all the data yet.
# timestamp must be TimestampType so the window() function can group by time.
schema = StructType() \
    .add('user_id',   StringType()) \
    .add('page',      StringType()) \
    .add('timestamp', TimestampType())

# ── READ STREAM ───────────────────────────────────────────────────────────────
# readStream.json() watches the folder and processes each new file as a micro-batch.
# In production with Kafka this would be:
#   spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers", "localhost:9092")
#     .option("subscribe", "page-views")
#     .load()
# followed by from_json() to parse the value bytes — same window/watermark logic applies.
stream_df = spark.readStream \
    .schema(schema) \
    .json(INPUT_DIR)

# ── WATERMARK + TUMBLING WINDOW ───────────────────────────────────────────────
# withWatermark("timestamp", "10 minutes"):
#   Spark tracks the maximum event timestamp it has seen.
#   It accepts late events up to 10 minutes behind that maximum.
#   Events older than (max_timestamp - 10 min) are dropped.
#   This bounds the state size — Spark can discard old windows from memory.
#
# window(col("timestamp"), "5 minutes"):
#   Groups events into 5-minute buckets based on the EVENT timestamp
#   (not the arrival time). A 09:43 event always lands in the 09:40-09:45 bucket.
#   Tumbling = non-overlapping: each event belongs to exactly one bucket.
#
# groupBy(window, page) + count():
#   Within each 5-minute bucket, count views per page.
windowed_counts = stream_df \
    .withWatermark('timestamp', '10 minutes') \
    .groupBy(
        window(col('timestamp'), '5 minutes'),
        col('page')
    ) \
    .count()

# ── WRITE TO CONSOLE ──────────────────────────────────────────────────────────
# outputMode='update' — print only rows that changed in this micro-batch.
# trigger='10 seconds' — process new files every 10 seconds.
# truncate=False — show full window timestamps without cutting them off.
query = windowed_counts.writeStream \
    .outputMode('update') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime='10 seconds') \
    .start()

print(f'\nStreaming started. Watching: {INPUT_DIR}')
print('Run q14_pageview_producer.py in another terminal.\n')

query.awaitTermination()
