# Q5: Basic Kafka Producer
from kafka import KafkaProducer
import json
import random

TOPIC = 'nifi-transactions'
TOTAL_RECORDS = 2500

# ── PRODUCER SETUP ────────────────────────────────────────────────────────────
# bootstrap_servers: address of the Kafka broker to connect to.
# value_serializer: Kafka only stores raw bytes, not Python objects.
#   This lambda converts each dict → JSON string → UTF-8 bytes automatically.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['retail', 'food', 'travel', 'electronics', 'health']

print(f'Sending {TOTAL_RECORDS} records to topic: {TOPIC}')

# ── EVENT GENERATION LOOP ─────────────────────────────────────────────────────
# producer.send() is non-blocking — it queues the message in an internal buffer.
# The background I/O thread batches and delivers them to the broker asynchronously.
# 3-in-4 chance of SUCCESS simulates a realistic error rate.
for i in range(1, TOTAL_RECORDS + 1):
    record = {
        'id': i,
        'customer_id': random.randint(1000, 9999),
        'amount': round(random.uniform(5.0, 2000.0), 2),
        'category': random.choice(categories),
        'status': random.choice(['SUCCESS', 'SUCCESS', 'SUCCESS', 'ERROR']),
    }

    producer.send(TOPIC, value=record)

    if i % 500 == 0:
        print(f'  Sent {i} / {TOTAL_RECORDS} records')

# ── FLUSH & CLOSE ─────────────────────────────────────────────────────────────
# flush() blocks until the broker acknowledges all buffered messages.
# Without it, the script could exit before all messages are delivered.
producer.flush()
producer.close()
print('Done.')
