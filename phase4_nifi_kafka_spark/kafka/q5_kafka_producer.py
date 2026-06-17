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

