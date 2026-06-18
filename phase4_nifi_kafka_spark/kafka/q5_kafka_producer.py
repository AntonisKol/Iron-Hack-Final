# Q5: Basic Kafka Producer
from kafka import KafkaProducer
import json
import random

TOPIC = 'nifi-transactions'
TOTAL_RECORDS = 2500

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['retail', 'food', 'travel', 'electronics', 'health']

print(f'Sending {TOTAL_RECORDS} records to topic: {TOPIC}')

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

producer.flush()
producer.close()
print('Done.')
