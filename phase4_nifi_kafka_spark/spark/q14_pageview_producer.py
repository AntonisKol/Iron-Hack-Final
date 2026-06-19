# Q14: Page View Producer
from kafka import KafkaProducer
from datetime import datetime, timedelta
import random
import json
import time

TOPIC = 'page-views'
PAGES = ['/home', '/products', '/checkout', '/about', '/contact']

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

print(f'Sending 30 page-view events to Kafka topic: {TOPIC}\n')
now = datetime.utcnow()

for i in range(30):
    minutes_ago = random.uniform(0, 10)
    event_time = now - timedelta(minutes=minutes_ago)
    event = {
        'user_id': f'U{random.randint(100, 999)}',
        'page': random.choice(PAGES),
        'timestamp': event_time.strftime('%Y-%m-%dT%H:%M:%S'),
    }
    producer.send(TOPIC, value=event)
    print(f'  [{i+1:02d}] {event["timestamp"]}  {event["user_id"]}  {event["page"]}')
    time.sleep(0.2)

producer.flush()
producer.close()
print('\nAll 30 events sent.')
