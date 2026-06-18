from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StringType, FloatType, ArrayType
)
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector
import json
import math
import os

# Step 6: join streaming events with USER_DIM and POST_DIM → write CURATED_EVENTS

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  'SOCIAL_MEDIA_DB',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

BASE_DIR   = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's6_checkpoint')

def nan_to_none(v):
    if v is None:
        return None
    try:
        if math.isnan(float(v)):
            return None
    except (TypeError, ValueError):
        pass
    return v


# ── LOAD DIMENSION TABLES INTO MEMORY ─────────────────────────────────────────
# Pattern: dict lookup instead of Spark broadcast join.
# USER_DIM (100 rows) and POST_DIM (500 rows) are small and stable — loading
# them into Python dicts at startup costs one Snowflake query total.
# A Spark broadcast join on a streaming DataFrame would work but adds complexity
# for no gain at this scale.
#
# Ownership: USER_DIM and POST_DIM are populated by event_simulator.py at startup
# (populate_dimensions()) before any events flow. This job only reads them —
# dimensions are reference data owned by the simulator, facts by the pipeline.
def load_dimensions():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    cur.execute('SELECT user_id, username, user_type FROM CURATED.USER_DIM')
    user_dim = {row[0]: {'username': row[1], 'user_type': row[2]} for row in cur.fetchall()}

    cur.execute('SELECT post_id, content_type, hashtags FROM CURATED.POST_DIM')
    post_dim = {}
    for row in cur.fetchall():
        post_dim[row[0]] = {
            'content_type': row[1],
            'hashtags':     row[2],  # already a string from Snowflake VARIANT
        }

    conn.close()
    print(f'Loaded {len(user_dim)} users and {len(post_dim)} posts into memory.')
    return user_dim, post_dim

user_dim, post_dim = load_dimensions()

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('S6 - Data Enrichment') \
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

# Design note: this job reads directly from Kafka, so it enriches all events
# including those that failed validation in s4. In a stricter production design
# s6 would read from RAW_EVENTS WHERE is_valid = TRUE so only clean events reach
# CURATED_EVENTS. For this capstone both jobs run independently on the same topic.
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


def enrich_and_write(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf     = batch_df.toPandas()
    conn    = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur     = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    rows    = []

    for _, row in pdf.iterrows():
        uid    = str(row.get('user_id') or '')
        pid    = str(row.get('post_id') or '') or None

        # look up user dimension — if not found, use empty defaults
        user_info = user_dim.get(uid, {'username': 'unknown', 'user_type': 'regular'})

        # look up post dimension — may not exist for FOLLOW/PROFILE_VISIT events
        post_info = post_dim.get(pid, {}) if pid else {}

        raw_tags = nan_to_none(row.get('hashtags'))
        if raw_tags is not None:
            hashtags_json = json.dumps(list(raw_tags) if raw_tags else [])
        elif post_info.get('hashtags'):
            # Snowflake VARIANT comes back as a string; re-dump it for PARSE_JSON
            try:
                hashtags_json = json.dumps(json.loads(str(post_info['hashtags'])))
            except (json.JSONDecodeError, TypeError):
                hashtags_json = '[]'
        else:
            hashtags_json = '[]'

        rows.append((
            str(row.get('event_id') or ''),
            str(row.get('event_type') or ''),
            uid,
            user_info['username'],
            user_info['user_type'],
            pid,
            post_info.get('content_type') or str(row.get('content_type') or '') or None,
            hashtags_json,
            str(row.get('comment_text') or '') or None,
            nan_to_none(row.get('video_duration_sec')),
            nan_to_none(row.get('watch_time_sec')),
            str(row.get('timestamp') or now_str),
            now_str,
        ))

    # executemany can't batch VARIANT inserts — individual execute per row
    for row_tuple in rows:
        cur.execute(
            'INSERT INTO CURATED.CURATED_EVENTS ('
            '  event_id, event_type, user_id, username, user_type,'
            '  post_id, content_type, hashtags,'
            '  comment_text, video_duration_sec, watch_time_sec,'
            '  event_timestamp, ingested_at'
            ') SELECT %s,%s,%s,%s,%s,%s,%s,PARSE_JSON(%s),%s,%s,%s,%s,%s',
            row_tuple,
        )
    conn.close()
    print(f'  Batch {batch_id}: {len(rows)} enriched events → CURATED_EVENTS')


query = stream_df.writeStream \
    .foreachBatch(enrich_and_write) \
    .trigger(processingTime='15 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Enrichment stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
