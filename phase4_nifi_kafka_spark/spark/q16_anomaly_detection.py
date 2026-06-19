# Q16: IoT Anomaly Detection — Spark Structured Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector
import json
import uuid
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

KAFKA_BROKER  = 'localhost:9092'
INPUT_TOPIC   = 'sensor-readings'
ALERT_TOPIC   = 'sensor-alerts'

BASE_DIR   = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, '../output/q16_checkpoint')

spark = SparkSession.builder \
    .appName('Q16 - IoT Anomaly Detection') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── SCHEMA ────────────────────────────────────────────────────────────────────
# Explicit schema required for from_json — Spark cannot infer from a streaming source.
schema = StructType() \
    .add('sensor_id',   StringType()) \
    .add('temperature', DoubleType()) \
    .add('ts',          TimestampType())

# ── READ FROM KAFKA ───────────────────────────────────────────────────────────
# startingOffsets='latest': only process new readings, ignore historical backlog.
# failOnDataLoss='false': continue if Kafka deletes old offsets (log compaction).
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER) \
    .option('subscribe', INPUT_TOPIC) \
    .option('startingOffsets', 'latest') \
    .option('failOnDataLoss', 'false') \
    .load()

# ── PARSE & FILTER ────────────────────────────────────────────────────────────
# from_json: decode the raw Kafka bytes into a struct using the declared schema.
# Null checks drop malformed messages before they reach detect_and_write.
stream_df = (
    raw_stream
    .select(from_json(col('value').cast('string'), schema).alias('d'))
    .select('d.*')
    .filter(col('sensor_id').isNotNull())
    .filter(col('temperature').isNotNull())
)


def init_tables():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SENSOR_READINGS (
            reading_id  VARCHAR,
            sensor_id   VARCHAR,
            temperature FLOAT,
            event_ts    TIMESTAMP,
            ingested_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SENSOR_ANOMALY_ALERTS (
            alert_id       VARCHAR,
            sensor_id      VARCHAR,
            temperature    FLOAT,
            rolling_avg    FLOAT,
            rolling_stddev FLOAT,
            threshold      FLOAT,
            event_ts       TIMESTAMP,
            detected_at    TIMESTAMP
        )
    """)
    conn.close()
    print('Tables ready: SENSOR_READINGS, SENSOR_ANOMALY_ALERTS\n')

init_tables()


# ── FOREACH BATCH HANDLER ─────────────────────────────────────────────────────
# foreachBatch gives each micro-batch as a regular (non-streaming) DataFrame.
# Four steps run per batch:
#   1. Persist readings to Snowflake — builds rolling history across batches.
#   2. Query Snowflake AVG + STDDEV_POP over the last hour, per sensor.
#   3. Flag readings where temperature > rolling_avg + 3 * rolling_stddev (3-sigma rule).
#   4. Write alerts to both Kafka (sensor-alerts) and Snowflake (SENSOR_ANOMALY_ALERTS).
def detect_and_write(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf     = batch_df.toPandas()
    conn    = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur     = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    for _, row in pdf.iterrows():
        cur.execute(
            'INSERT INTO SENSOR_READINGS '
            '(reading_id, sensor_id, temperature, event_ts, ingested_at) '
            'VALUES (%s, %s, %s, %s, %s)',
            (str(uuid.uuid4()), str(row['sensor_id']),
             float(row['temperature']), str(row['ts']), now_str),
        )

    # ── COMPUTE ROLLING STATS PER SENSOR ──────────────────────────────────────
    # Query last-hour history from Snowflake — history accumulates across batches.
    # r[2] >= 10: require at least 10 readings before flagging anomalies.
    #   Prevents false alarms during the first few seconds a sensor comes online.
    sensors = pdf['sensor_id'].unique().tolist()
    stats   = {}
    for sensor in sensors:
        cur.execute(
            'SELECT AVG(temperature), STDDEV_POP(temperature), COUNT(*) '
            'FROM SENSOR_READINGS '
            'WHERE sensor_id = %s '
            "  AND event_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())",
            (sensor,),
        )
        r = cur.fetchone()
        if r and r[2] >= 10 and r[0] is not None:
            stats[sensor] = {'avg': float(r[0]), 'std': float(r[1] or 0)}

    alerts = []
    for _, row in pdf.iterrows():
        sid  = str(row['sensor_id'])
        temp = float(row['temperature'])
        s    = stats.get(sid)
        if s and s['std'] > 0:
            threshold = s['avg'] + 3 * s['std']
            if temp > threshold:
                alerts.append({
                    'alert_id':       str(uuid.uuid4()),
                    'sensor_id':      sid,
                    'temperature':    temp,
                    'rolling_avg':    round(s['avg'], 3),
                    'rolling_stddev': round(s['std'], 3),
                    'threshold':      round(threshold, 3),
                    'event_ts':       str(row['ts']),
                    'detected_at':    now_str,
                })

    if alerts:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                acks='all',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            for a in alerts:
                producer.send(ALERT_TOPIC, value=a, key=a['sensor_id'].encode())
            producer.flush()
            producer.close()
        except Exception as e:
            print(f'  [WARN] Kafka alert send failed: {e}')

        for a in alerts:
            cur.execute(
                'INSERT INTO SENSOR_ANOMALY_ALERTS '
                '(alert_id, sensor_id, temperature, rolling_avg, '
                ' rolling_stddev, threshold, event_ts, detected_at) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (a['alert_id'], a['sensor_id'], a['temperature'],
                 a['rolling_avg'], a['rolling_stddev'], a['threshold'],
                 a['event_ts'], a['detected_at']),
            )
            print(f'  ANOMALY  {a["sensor_id"]}  '
                  f'{a["temperature"]:.1f}C  '
                  f'(threshold {a["threshold"]:.1f}C)')

        print(f'  Batch {batch_id}: {len(alerts)} alerts → '
              f'Kafka "{ALERT_TOPIC}" + Snowflake')
    else:
        print(f'  Batch {batch_id}: {len(pdf)} readings — no anomalies')

    conn.close()


# ── START STREAMING QUERY ─────────────────────────────────────────────────────
# trigger='30 seconds': collect Kafka messages for 30 seconds, then run detect_and_write.
# checkpointLocation: Spark tracks which Kafka offsets were processed — safe to restart.
query = stream_df.writeStream \
    .foreachBatch(detect_and_write) \
    .trigger(processingTime='30 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print(f'Q16 anomaly detection started.')
print(f'  Reading from Kafka: {INPUT_TOPIC}')
print(f'  Alerts → Kafka: {ALERT_TOPIC}  +  Snowflake: SENSOR_ANOMALY_ALERTS')
print('Run q19_sensor_producer.py to generate sensor data.\n')

query.awaitTermination()
