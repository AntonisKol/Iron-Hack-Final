# Steps 4 & 5: Real-Time Processing — RAW Ingestion & Validation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, from_json
from datetime import datetime
import snowflake.connector
import json
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema, nan_to_none

# ── VALID EVENT TYPES ─────────────────────────────────────────────────────────
# Any event_type not in this set is marked invalid but still stored in RAW.
# Nothing is discarded — the raw layer is a complete, immutable audit log.
VALID_EVENT_TYPES = {
    'POST_CREATED', 'LIKE', 'COMMENT', 'SHARE',
    'FOLLOW', 'VIDEO_VIEW', 'PROFILE_VISIT',
}

BASE_DIR   = os.path.dirname(__file__)
BAD_DIR    = os.path.join(BASE_DIR, '..', 'data', 'bad_records')
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's4_checkpoint')

os.makedirs(BAD_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName('S4+S5 - Stream Processor & RAW Layer') \
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


# ── VALIDATION RULES ──────────────────────────────────────────────────────────
# Returns a list of error strings — empty list means the event is valid.
# post_id is required for engagement events (LIKE, COMMENT, SHARE, VIDEO_VIEW)
# but not for social events (FOLLOW, PROFILE_VISIT, POST_CREATED).
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


# ── FOREACH BATCH HANDLER — RAW WRITE ────────────────────────────────────────
# Every event (valid or not) is written to RAW.RAW_EVENTS.
# Invalid events are ALSO saved locally to bad_records/ for investigation.
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
        raw_hashtags = nan_to_none(row_dict.get('hashtags'))
        hashtags = json.dumps(list(raw_hashtags) if raw_hashtags is not None else [])

        if is_valid:
            valid_cnt += 1
        else:
            invalid_cnt += 1
            # Save a copy of bad events locally — raw_payload + error list for debugging
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


query = stream_df.writeStream \
    .foreachBatch(write_to_raw) \
    .trigger(processingTime='10 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Stream processor started. Consuming from Kafka topic: social-events')
print('Start the simulator in another terminal:\n  python3 simulator/event_simulator.py\n')

query.awaitTermination()
