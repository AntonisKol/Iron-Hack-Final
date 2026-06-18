from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, lit, from_json
from pyspark.sql.types import StructType, StringType, ArrayType, FloatType
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector
import os

# Step 8: detect posts with >500 LIKE events in a 5-minute window → ANALYTICS.VIRAL_POSTS
# alert is printed to console; threshold and window size are configurable constants

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  'SOCIAL_MEDIA_DB',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

VIRAL_THRESHOLD = 500  # likes within 5 minutes

BASE_DIR   = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's8_checkpoint')

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('S8 - Viral Post Detection') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
_log4j = spark.sparkContext._jvm.org.apache.log4j
_log4j.Logger.getLogger('org.apache.spark.sql.kafka010.KafkaDataConsumer').setLevel(_log4j.Level.ERROR)

schema = StructType() \
    .add('event_id',           StringType()) \
    .add('event_type',         StringType()) \
    .add('user_id',            StringType()) \
    .add('post_id',            StringType()) \
    .add('target_user_id',     StringType()) \
    .add('hashtags',           ArrayType(StringType())) \
    .add('comment_text',       StringType()) \
    .add('content_type',       StringType()) \
    .add('video_duration_sec', FloatType()) \
    .add('watch_time_sec',     FloatType()) \
    .add('timestamp',          StringType())

# ── AGGREGATE: LIKE counts per post per 5-minute window ──────────────────────
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

# Aggregation counts ALL posts — not just viral ones. Spark must track every
# post's like count in window state to know the moment any of them cross 500.
# Filtering before the aggregation would only count events for already-viral
# posts, making detection impossible.
like_counts = (
    stream_df
    .filter(col('event_type') == 'LIKE')
    .filter(col('post_id').isNotNull())
    .groupBy(window(col('ts'), '5 minutes'), col('post_id'))
    .agg(count('*').alias('like_count'))
)
# outputMode('update'): as a post's like count grows, Spark re-emits that row
# with the updated count. Each re-emission triggers detect_and_write, which
# applies the threshold filter — so only crossings at or above 500 write to
# Snowflake. The viral_burst() in the simulator (150 likes in ~1.5s) is what
# reliably triggers this threshold in the demo.


def detect_and_write(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # threshold filter happens here, not before writeStream — see comment above
    viral_df = batch_df.filter(col('like_count') >= VIRAL_THRESHOLD)
    if viral_df.isEmpty():
        return

    pdf     = viral_df.toPandas()
    conn    = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur     = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    rows    = []

    for _, row in pdf.iterrows():
        w_start  = str(row['window']['start'])
        w_end    = str(row['window']['end'])
        post_id  = str(row['post_id'])
        likes    = int(row['like_count'])

        print(f'  🔥 VIRAL DETECTED: {post_id} — {likes} likes in window {w_start} → {w_end}')

        rows.append((post_id, w_start, w_end, likes, now_str))

    cur.executemany(
        'INSERT INTO ANALYTICS.VIRAL_POSTS '
        '(post_id, window_start, window_end, like_count, detected_at) '
        'VALUES (%s, %s, %s, %s, %s)',
        rows,
    )
    conn.close()
    print(f'  Batch {batch_id}: {len(rows)} viral posts → VIRAL_POSTS')


query = like_counts.writeStream \
    .outputMode('update') \
    .foreachBatch(detect_and_write) \
    .trigger(processingTime='10 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print(f'Viral detection stream started. Threshold: {VIRAL_THRESHOLD} likes / 5 min')
print('Consuming from Kafka topic: social-events\n')
query.awaitTermination()
