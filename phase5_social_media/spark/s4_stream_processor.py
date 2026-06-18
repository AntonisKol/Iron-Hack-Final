from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, from_json
from pyspark.sql.types import (
    StructType, StringType, FloatType, TimestampType, ArrayType
)
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector
import json
import math
import os

# Steps 4 + 5: consume events, validate, write to Snowflake RAW_EVENTS

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  'SOCIAL_MEDIA_DB',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

VALID_EVENT_TYPES = {
    'POST_CREATED', 'LIKE', 'COMMENT', 'SHARE',
    'FOLLOW', 'VIDEO_VIEW', 'PROFILE_VISIT',
}

BASE_DIR   = os.path.dirname(__file__)
BAD_DIR    = os.path.join(BASE_DIR, '..', 'data', 'bad_records')
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's4_checkpoint')

os.makedirs(BAD_DIR, exist_ok=True)

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('S4+S5 - Stream Processor & RAW Layer') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
_log4j = spark.sparkContext._jvm.org.apache.log4j
_log4j.Logger.getLogger('org.apache.spark.sql.kafka010.KafkaDataConsumer').setLevel(_log4j.Level.ERROR)

# ── SCHEMA ────────────────────────────────────────────────────────────────────
# explicit schema required for readStream — must match what the simulator writes
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
    .add('timestamp',          StringType())   # stored as string, cast to timestamp in SQL

# ── READ STREAM ───────────────────────────────────────────────────────────────
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

def nan_to_none(v):
    """pandas fills missing float columns with NaN; Snowflake rejects 'NAN' as a literal."""
    if v is None:
        return None
    try:
        if math.isnan(float(v)):
            return None
    except (TypeError, ValueError):
        pass
    return v


# ── VALIDATION ────────────────────────────────────────────────────────────────
# Step 4: data quality checks — mark invalid records instead of dropping them.
# Malformed records land in RAW_EVENTS with is_valid=FALSE and a reason.
# This preserves the full audit trail — nothing is silently discarded.
def validate_row(row):
    errors = []
    if not row.get('event_id'):
        errors.append('missing event_id')
    if not row.get('event_type') or row.get('event_type') not in VALID_EVENT_TYPES:
        errors.append(f'invalid event_type: {row.get("event_type")}')
    if not row.get('user_id'):
        errors.append('missing user_id')
    if row.get('event_type') in ('LIKE', 'COMMENT', 'SHARE', 'VIDEO_VIEW') and not row.get('post_id'):
        errors.append('missing post_id for event type that requires it')
    return errors


# ── WRITE TO SNOWFLAKE RAW ────────────────────────────────────────────────────
def write_to_raw(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf        = batch_df.toPandas()
    conn       = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur        = conn.cursor()
    now_str    = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    valid_cnt  = 0
    invalid_cnt = 0

    for _, row in pdf.iterrows():
        row_dict = row.to_dict()
        errors   = validate_row(row_dict)
        is_valid = len(errors) == 0
        hashtags = json.dumps(row_dict.get('hashtags') or [])

        if is_valid:
            valid_cnt += 1
        else:
            invalid_cnt += 1
            # save bad records locally for investigation
            bad_path = os.path.join(BAD_DIR, f'bad_{batch_id}_{row_dict.get("event_id","x")}.json')
            with open(bad_path, 'w') as f:
                json.dump({**row_dict, 'errors': errors}, f)

        vid_dur     = nan_to_none(row_dict.get('video_duration_sec'))
        watch       = nan_to_none(row_dict.get('watch_time_sec'))
        clean_dict  = {**row_dict, 'video_duration_sec': vid_dur, 'watch_time_sec': watch}

        cur.execute(
            'INSERT INTO RAW.RAW_EVENTS ('
            '  event_id, event_type, user_id, post_id, target_user_id,'
            '  hashtags, comment_text, content_type,'
            '  video_duration_sec, watch_time_sec,'
            '  raw_payload, event_timestamp, ingested_at, is_valid, validation_errors'
            ') SELECT %s,%s,%s,%s,%s, PARSE_JSON(%s),%s,%s, %s,%s, PARSE_JSON(%s),%s,%s,%s,%s',
            (
                str(row_dict.get('event_id') or ''),
                str(row_dict.get('event_type') or ''),
                str(row_dict.get('user_id') or ''),
                str(row_dict.get('post_id') or '') or None,
                str(row_dict.get('target_user_id') or '') or None,
                hashtags,
                str(row_dict.get('comment_text') or '') or None,
                str(row_dict.get('content_type') or '') or None,
                vid_dur,
                watch,
                json.dumps(clean_dict, default=str),
                str(row_dict.get('timestamp') or now_str),
                now_str,
                is_valid,
                '; '.join(errors) if errors else None,
            ),
        )

    conn.close()
    print(f'  Batch {batch_id}: {valid_cnt} valid, {invalid_cnt} invalid → RAW_EVENTS')


# ── WRITE STREAM ──────────────────────────────────────────────────────────────
query = stream_df.writeStream \
    .foreachBatch(write_to_raw) \
    .trigger(processingTime='10 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Stream processor started. Consuming from Kafka topic: social-events')
print('Start the simulator in another terminal:\n  python3 simulator/event_simulator.py\n')

query.awaitTermination()
