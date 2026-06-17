from datetime import datetime, timedelta
import random
import json
import time
import os

INPUT_DIR = os.path.join(os.path.dirname(__file__), 'pageviews_input')
PAGES = ['/home', '/products', '/checkout', '/about', '/contact']

now = datetime.utcnow()
print(f'Writing page-view events to: {INPUT_DIR}\n')

for i in range(30):
    # spread events across the last 10 minutes — some will fall in the
    # current 5-min window, some in the previous one
    minutes_ago = random.uniform(0, 10)
    event_time = now - timedelta(minutes=minutes_ago)

    event = {
        'user_id':   f'U{random.randint(100, 999)}',
        'page':      random.choice(PAGES),
        'timestamp': event_time.strftime('%Y-%m-%dT%H:%M:%S'),
    }

    # each event is written as its own JSON file — Spark picks it up in the next batch
    filename = os.path.join(INPUT_DIR, f'event_{i:03d}_{int(time.time()*1000)}.json')
    with open(filename, 'w') as f:
        json.dump(event, f)

    print(f"  {event['timestamp']}  {event['user_id']}  {event['page']}")
    time.sleep(0.2)

print('\nAll 30 events written.')
