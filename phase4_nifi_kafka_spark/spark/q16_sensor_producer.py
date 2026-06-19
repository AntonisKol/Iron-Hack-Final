# Q16: Kafka Sensor Producer — sends temperature readings to 'sensor-readings' topic
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone

TOPIC = 'sensor-readings'
SENSORS = ['S001', 'S002', 'S003']

# ── PRODUCER SETUP ────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

print(f'Sending sensor readings to Kafka topic: {TOPIC}')
print(f'Sensors: {SENSORS}')
print(f'Normal range: 18–28°C  |  Anomaly spike: 60–80°C (injected every ~30 readings)')
print('Press Ctrl+C to stop.\n')

count = 0

try:
    while True:
        sensor_id = random.choice(SENSORS)

        # ── ANOMALY INJECTION ─────────────────────────────────────────────────
        # Every ~30 readings, inject a spike well above the 3-sigma threshold.
        # Spark's anomaly detection only fires after ≥10 readings per sensor,
        # so the first anomaly alert appears after ~10+ normal readings.
        if count > 0 and count % 30 == 0:
            temperature = round(random.uniform(60.0, 80.0), 2)
            label = '*** SPIKE ***'
        else:
            temperature = round(random.uniform(18.0, 28.0), 2)
            label = ''

        ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

        record = {
            'sensor_id': sensor_id,
            'temperature': temperature,
            'ts': ts,
        }

        producer.send(TOPIC, value=record)
        print(f'  [{ts}]  {sensor_id}  {temperature}°C  {label}')

        count += 1
        time.sleep(0.5)

except KeyboardInterrupt:
    print(f'\nStopped after {count} readings.')
    producer.flush()
    producer.close()
