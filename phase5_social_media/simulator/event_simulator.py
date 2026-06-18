import json
import os
import random
import time
import uuid
from datetime import datetime

from dotenv import load_dotenv
import snowflake.connector

# graceful fallback — simulator works even if kafka-python is not installed
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print('[WARN] kafka-python not installed — events will only write to files')

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
NUM_USERS         = 100
NUM_POSTS         = 500
NUM_INFLUENCERS   = 10   # first 10 users are influencers (higher engagement target)
NUM_VIRAL_POSTS   = 50   # first 50 posts are viral candidates (higher engagement)
EVENTS_PER_MINUTE = 1000
SLEEP_INTERVAL    = 60.0 / EVENTS_PER_MINUTE  # 0.06 seconds between events
VIRAL_BURST_SIZE  = 600  # likes sent during a viral burst — must exceed s8's VIRAL_THRESHOLD (500)
KAFKA_TOPIC       = 'social-events'

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  'SOCIAL_MEDIA_DB',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

# ── STATIC REFERENCE DATA ─────────────────────────────────────────────────────
HASHTAG_POOL = [
    '#trending', '#viral', '#instagood', '#photography', '#fashion',
    '#travel', '#food', '#fitness', '#music', '#technology',
    '#art', '#nature', '#love', '#lifestyle', '#business',
    '#motivation', '#design', '#marketing', '#coding', '#datascience',
    '#AI', '#machinelearning', '#python', '#analytics', '#cloud',
    '#startup', '#innovation', '#crypto', '#sustainability', '#reels',
]

CONTENT_TYPES = ['image', 'video', 'text', 'reel', 'story']
COUNTRIES     = ['US', 'UK', 'DE', 'FR', 'IN', 'BR', 'JP', 'CA', 'AU', 'MX']

POSITIVE_COMMENTS = [
    'Love this! 🔥', 'Amazing content!', 'This is incredible!',
    'Best post today!', 'Absolutely stunning!', 'Keep it up!',
    'This made my day!', 'Fantastic work!', 'You are the best!',
    'So inspiring!', 'Incredible job!', 'This is wonderful!',
]
NEGATIVE_COMMENTS = [
    'This is terrible', 'Worst content ever', 'Disappointed',
    'Do not like this at all', 'Horrible quality', 'This is awful',
    'Not impressed', 'Waste of time',
]
NEUTRAL_COMMENTS = [
    'Interesting', 'OK I guess', 'Not sure about this',
    'Maybe', 'Could be better', 'What do you think?',
    'I have seen this before', 'Thanks for sharing', 'Fair enough',
    'Noted', 'Will check later', 'Following',
]

# comment pool: 70% positive, 15% negative, 15% neutral (realistic skew)
COMMENT_POOL = (
    POSITIVE_COMMENTS * 7 +
    NEGATIVE_COMMENTS * 2 +
    NEUTRAL_COMMENTS  * 2
)

# event type weights — LIKE dominates, POST_CREATED is rare
EVENT_TYPE_POOL = (
    ['LIKE']          * 35 +
    ['VIDEO_VIEW']    * 25 +
    ['PROFILE_VISIT'] * 15 +
    ['COMMENT']       * 10 +
    ['SHARE']         *  7 +
    ['FOLLOW']        *  5 +
    ['POST_CREATED']  *  3
)


def make_user_id(n):
    return f'U{n:04d}'

def make_post_id(n):
    return f'P{n:04d}'


# ── STEP 1: POPULATE DIMENSION TABLES ────────────────────────────────────────
def populate_dimensions():
    """
    Insert USER_DIM and POST_DIM into Snowflake before streaming starts.
    These are static reference tables used for enrichment in Step 6.
    """
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()
    cur.execute('USE SCHEMA CURATED')

    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # users — build all rows first, insert in one batch
    print(f'Inserting {NUM_USERS} users into USER_DIM...')
    cur.execute('TRUNCATE TABLE USER_DIM')
    user_rows = []
    for i in range(1, NUM_USERS + 1):
        uid       = make_user_id(i)
        utype     = 'influencer' if i <= NUM_INFLUENCERS else 'regular'
        followers = random.randint(10_000, 2_000_000) if utype == 'influencer' else random.randint(50, 5_000)
        user_rows.append((
            uid, f'user_{uid.lower()}', utype, followers,
            random.randint(100, 1000), random.randint(10, 500),
            random.choice(COUNTRIES), now,
        ))
    cur.executemany(
        'INSERT INTO USER_DIM (user_id, username, user_type, follower_count, following_count, post_count, country, created_at) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        user_rows,
    )

    # posts — PARSE_JSON requires SELECT form; batch with executemany
    print(f'Inserting {NUM_POSTS} posts into POST_DIM...')
    cur.execute('TRUNCATE TABLE POST_DIM')
    # executemany can't batch VARIANT inserts — individual execute per row
    for i in range(1, NUM_POSTS + 1):
        pid   = make_post_id(i)
        uid   = make_user_id(random.randint(1, NUM_USERS))
        ctype = random.choice(CONTENT_TYPES)
        tags  = json.dumps(random.sample(HASHTAG_POOL, k=random.randint(1, 5)))
        cur.execute(
            'INSERT INTO POST_DIM (post_id, user_id, content_type, hashtags, created_at) '
            'SELECT %s, %s, %s, PARSE_JSON(%s), %s',
            (pid, uid, ctype, tags, now),
        )

    conn.close()
    print('Dimension tables populated.\n')


# ── STEP 2: EVENT GENERATOR ───────────────────────────────────────────────────
def generate_event(event_type=None, viral_post_id=None):
    """Build one event dict. If viral_post_id is set, force a LIKE for that post."""
    if viral_post_id:
        event_type = 'LIKE'
        post_id    = viral_post_id
    else:
        event_type = event_type or random.choice(EVENT_TYPE_POOL)
        # viral posts get picked 5× more often
        post_num   = random.randint(1, NUM_VIRAL_POSTS) if random.random() < 0.3 else random.randint(1, NUM_POSTS)
        post_id    = make_post_id(post_num)

    user_id = make_user_id(random.randint(1, NUM_USERS))
    now     = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    event = {
        'event_id':   str(uuid.uuid4()),
        'event_type': event_type,
        'user_id':    user_id,
        'timestamp':  now,
    }

    if event_type in ('LIKE', 'COMMENT', 'SHARE', 'VIDEO_VIEW', 'POST_CREATED'):
        event['post_id'] = post_id

    if event_type == 'FOLLOW':
        event['target_user_id'] = make_user_id(random.randint(1, NUM_USERS))

    if event_type == 'PROFILE_VISIT':
        event['target_user_id'] = make_user_id(random.randint(1, NUM_USERS))

    if event_type == 'POST_CREATED':
        event['hashtags']     = random.sample(HASHTAG_POOL, k=random.randint(1, 5))
        event['content_type'] = random.choice(CONTENT_TYPES)

    if event_type == 'COMMENT':
        event['comment_text'] = random.choice(COMMENT_POOL)

    if event_type == 'VIDEO_VIEW':
        duration           = random.randint(15, 180)
        event['video_duration_sec'] = duration
        event['watch_time_sec']     = round(random.uniform(5, duration), 1)

    return event


def write_event(event, producer=None):
    """Send event to Kafka topic 'social-events'. All Spark jobs consume from Kafka directly."""
    payload = json.dumps(event).encode('utf-8')

    if producer:
        try:
            producer.send(KAFKA_TOPIC, value=payload, key=event['event_id'].encode())
        except Exception as e:
            print(f'[Kafka] send failed: {e}')


# ── STEP 3: VIRAL BURST ───────────────────────────────────────────────────────
def viral_burst(producer=None):
    """
    Simulate a viral post: send VIRAL_BURST_SIZE LIKE events to the same post
    in rapid succession. This will trigger the viral detection pipeline (Step 8).
    """
    viral_post_id = make_post_id(random.randint(1, NUM_VIRAL_POSTS))
    print(f'\n🔥 VIRAL BURST → {viral_post_id} ({VIRAL_BURST_SIZE} likes)\n')
    for _ in range(VIRAL_BURST_SIZE):
        event = generate_event(viral_post_id=viral_post_id)
        write_event(event, producer)
        time.sleep(0.01)  # 100 events/second during burst


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    print('=== Social Media Event Simulator ===')
    print(f'Target rate : {EVENTS_PER_MINUTE} events/minute')

    # populate dimension tables first
    populate_dimensions()

    # connect to Kafka if available
    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            print('Kafka producer connected.\n')
        except Exception as e:
            print(f'[WARN] Kafka not reachable ({e}) — events will be dropped\n')

    total_sent  = 0
    burst_every = 60   # trigger a viral burst every ~60 seconds

    print('Streaming events... (Ctrl+C to stop)\n')
    print(f'{"#":>8}  {"Type":<15}  {"User":<8}  {"Post/Target":<10}  Timestamp')
    print('-' * 70)

    start_time = time.time()

    try:
        while True:
            event = generate_event()
            write_event(event, producer)
            total_sent += 1

            if total_sent % 100 == 0:
                elapsed  = time.time() - start_time
                rate_min = total_sent / elapsed * 60
                print(f'  [{total_sent:>7}]  {event["event_type"]:<15}  {event["user_id"]}  '
                      f'{event.get("post_id", event.get("target_user_id", "—")):<10}  '
                      f'{event["timestamp"]}  ({rate_min:.0f} evt/min)')

            # trigger a viral burst every burst_every seconds
            elapsed = time.time() - start_time
            if int(elapsed) > 0 and int(elapsed) % burst_every == 0 and total_sent % 10 == 0:
                viral_burst(producer)

            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        print(f'\nStopped after {total_sent} events.')

    finally:
        if producer:
            producer.flush()
            producer.close()


if __name__ == '__main__':
    main()
