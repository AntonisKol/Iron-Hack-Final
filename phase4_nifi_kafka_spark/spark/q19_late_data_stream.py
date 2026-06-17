from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector
import os

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  os.getenv('SNOWFLAKE_DATABASE'),
    'schema':    os.getenv('SNOWFLAKE_SCHEMA'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

BASE_DIR    = os.path.dirname(__file__)
INPUT_DIR   = os.path.join(BASE_DIR, 'q19_sensor_input')
CHECKPOINT  = os.path.join(BASE_DIR, 'output', 'q19_checkpoint')

os.makedirs(INPUT_DIR, exist_ok=True)

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('Q19 - Late Data Handling') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── SCHEMA ────────────────────────────────────────────────────────────────────
schema = StructType() \
    .add('sensor_id',   StringType()) \
    .add('temperature', DoubleType()) \
    .add('ts',          TimestampType())

# ── READ STREAM ───────────────────────────────────────────────────────────────
stream_df = spark.readStream \
    .schema(schema) \
    .json(INPUT_DIR)

# ── WATERMARK + WINDOW AGGREGATION ────────────────────────────────────────────
# withWatermark: accept events up to 10 minutes late
# window("ts", "5 minutes"): group by 5-minute tumbling buckets on the EVENT timestamp
# outputMode("update"): emit a window row every time it changes (not just when finalized)
aggregated = (
    stream_df
    .withWatermark('ts', '10 minutes')
    .groupBy(
        window(col('ts'), '5 minutes'),
        col('sensor_id'),
    )
    .agg(
        avg('temperature').alias('avg_temp'),
        count('*').alias('reading_count'),
    )
)

# ── SNOWFLAKE TABLE SETUP ─────────────────────────────────────────────────────
def init_tables():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    # latest result per window per sensor — upserted on every Spark batch
    cur.execute("""
        CREATE TABLE IF NOT EXISTS SENSOR_AGGREGATES (
            window_start  TIMESTAMP,
            window_end    TIMESTAMP,
            sensor_id     VARCHAR,
            avg_temp      FLOAT,
            reading_count INTEGER,
            last_updated  TIMESTAMP,
            version       INTEGER
        )
    """)

    # full audit trail — one row per version per window per sensor (never deleted)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS SENSOR_CORRECTIONS_HISTORY (
            window_start  TIMESTAMP,
            window_end    TIMESTAMP,
            sensor_id     VARCHAR,
            avg_temp      FLOAT,
            reading_count INTEGER,
            recorded_at   TIMESTAMP,
            version       INTEGER,
            is_correction BOOLEAN
        )
    """)

    conn.close()
    print('Snowflake tables ready: SENSOR_AGGREGATES, SENSOR_CORRECTIONS_HISTORY\n')

init_tables()

# ── UPSERT FUNCTION (called once per micro-batch) ─────────────────────────────
def upsert_to_snowflake(batch_df, batch_id):
    """
    foreachBatch receives each micro-batch as a regular (non-streaming) DataFrame.
    We flatten the window struct, collect to pandas, then do per-row Snowflake writes.
    """
    if batch_df.isEmpty():
        return

    # flatten window struct {start, end} into two plain columns
    flat_df = batch_df.select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('sensor_id'),
        col('avg_temp'),
        col('reading_count'),
    )

    # toPandas() is an ACTION — triggers Spark computation for this batch
    pdf = flat_df.toPandas()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()
    now  = datetime.utcnow()

    # Snowflake connector cannot bind pandas Timestamp objects directly —
    # convert everything to plain Python strings/floats before executing.
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')

    for _, row in pdf.iterrows():
        w_start = str(row['window_start'])   # pandas Timestamp → string
        w_end   = str(row['window_end'])
        sid     = str(row['sensor_id'])
        avg_t   = float(row['avg_temp'])
        cnt     = int(row['reading_count'])

        # does this window already exist? (i.e., is this a late-data correction?)
        cur.execute(
            'SELECT version FROM SENSOR_AGGREGATES '
            'WHERE window_start = %s AND sensor_id = %s',
            (w_start, sid),
        )
        existing = cur.fetchone()

        if existing:
            # late event arrived — update the existing aggregate
            new_version   = existing[0] + 1
            is_correction = True
            cur.execute(
                'UPDATE SENSOR_AGGREGATES '
                'SET avg_temp = %s, reading_count = %s, last_updated = %s, version = %s '
                'WHERE window_start = %s AND sensor_id = %s',
                (avg_t, cnt, now_str, new_version, w_start, sid),
            )
        else:
            # first time we see this window — insert
            new_version   = 1
            is_correction = False
            cur.execute(
                'INSERT INTO SENSOR_AGGREGATES '
                '(window_start, window_end, sensor_id, avg_temp, reading_count, last_updated, version) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (w_start, w_end, sid, avg_t, cnt, now_str, new_version),
            )

        # always append to history — preserves every version for Power BI corrections view
        cur.execute(
            'INSERT INTO SENSOR_CORRECTIONS_HISTORY '
            '(window_start, window_end, sensor_id, avg_temp, reading_count, '
            ' recorded_at, version, is_correction) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
            (w_start, w_end, sid, avg_t, cnt, now_str, new_version, is_correction),
        )

        tag = 'CORRECTION v' + str(new_version) if is_correction else 'NEW        v1'
        print(f'  [{tag}]  {sid}  {w_start}–{w_end}  avg={avg_t:.2f}°C  n={cnt}')

    conn.close()

# ── WRITE STREAM ──────────────────────────────────────────────────────────────
query = (
    aggregated.writeStream
    .outputMode('update')
    .foreachBatch(upsert_to_snowflake)
    .trigger(processingTime='10 seconds')
    .option('checkpointLocation', CHECKPOINT)
    .start()
)

print(f'Streaming started. Watching: {INPUT_DIR}')
print('Run q19_sensor_producer.py in another terminal.\n')

query.awaitTermination()
