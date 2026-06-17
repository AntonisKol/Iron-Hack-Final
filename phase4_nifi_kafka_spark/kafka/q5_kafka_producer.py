"""
Kafka producer for NiFi Q5.
Sends 2,500 JSON transaction records to the 'nifi-transactions' topic.
NiFi consumes them with ConsumeKafka and merges every 1,000 records
(or every 10 seconds) into a single JSON array using MergeContent.

Run with:
    python3 phase4_nifi_kafka_spark/kafka/kafka_producer_q5.py

──────────────────────────────────────────────────────────────────
NiFi flow settings (not visible in this code):
──────────────────────────────────────────────────────────────────
ConsumeKafka:
  - Kafka Connection Service → KafkaConnectionService
      Bootstrap Servers: localhost:9092   ← same broker this script sends to
  - Group ID: nifi-consumer               ← Kafka tracks this consumer's offset
  - Topics: nifi-transactions             ← same topic this script writes to
  - Auto Offset Reset: earliest           ← if NiFi restarts, re-read from start
  - Each Kafka message becomes one FlowFile in NiFi

MergeContent:
  - Minimum / Maximum Number of Entries: 1000
      → collects 1,000 FlowFiles then releases them as one merged file
  - Maximum Bin Age: 10 sec
      → if fewer than 1,000 arrive within 10 seconds, releases whatever it has
        (prevents data sitting in NiFi forever during low-traffic periods)
  - Header: [   Demarcator: ,   Footer: ]
      → wraps the 1,000 JSON objects into a valid JSON array: [{...},{...},...]

PutFile (replaces PutHDFS in this local setup):
  - Directory: phase4_nifi_kafka_spark/kafka/output
  - In production this would be PutHDFS writing to /data/transactions/
  - Conflict Resolution: replace
──────────────────────────────────────────────────────────────────
"""

from kafka import KafkaProducer   # kafka-python client library
import json
import random

TOPIC = 'nifi-transactions'  # Kafka topic NiFi is listening to
TOTAL_RECORDS = 2500          # total messages to send — produces 2 full batches + 1 partial

# KafkaProducer connects to the broker at localhost:9092
# value_serializer converts each Python dict to UTF-8 encoded JSON bytes
# Kafka only stores bytes — serialization happens on the producer side
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['retail', 'food', 'travel', 'electronics', 'health']

print(f'Sending {TOTAL_RECORDS} records to topic: {TOPIC}')

for i in range(1, TOTAL_RECORDS + 1):
    # build one transaction record — each becomes one Kafka message → one NiFi FlowFile
    record = {
        'id': i,
        'customer_id': random.randint(1000, 9999),          # random customer
        'amount': round(random.uniform(5.0, 2000.0), 2),    # transaction value in euros
        'category': random.choice(categories),               # merchant category
        'status': random.choice(['SUCCESS', 'SUCCESS', 'SUCCESS', 'ERROR']),  # 75% success rate
    }

    # send() is async — it puts the message in an internal buffer, not sent yet
    # Kafka batches messages internally for efficiency before flushing to the broker
    producer.send(TOPIC, value=record)

    if i % 500 == 0:
        print(f'  Sent {i} / {TOTAL_RECORDS} records')

# flush() forces all buffered messages to be sent to the broker immediately
# without this, the last batch might never reach Kafka if the script exits too fast
producer.flush()

producer.close()  # clean up the connection
print('Done.')

