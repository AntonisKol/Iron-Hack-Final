# Phase 5 — End-to-End Social Media Analytics Platform
**Presentation Guide**

---

## What Was Asked (Technical Brief)

Design and build a complete real-time analytics platform for a simulated social media application — from scratch, end to end. The platform must: generate realistic user events, ingest them into a Kafka topic, process them with multiple concurrent Spark Structured Streaming jobs, persist results in a three-layer Snowflake data warehouse (RAW → CURATED → ANALYTICS), and visualise the output in Tableau dashboards. Every component must be wired together and working simultaneously.

This is the capstone of the programme — it uses all prior technologies together.

---

## Architecture in One Line

> Simulator → Kafka → 6 parallel Spark jobs → 3-layer Snowflake → Tableau

---

## Technical Breakdown — File by File

---

### `snowflake/setup.sql` — Database & Schema Initialisation

One-time setup that creates the entire Snowflake warehouse structure. `CREATE DATABASE IF NOT EXISTS SOCIAL_MEDIA_DB` establishes the database. Three schemas are created: `RAW` holds `RAW_EVENTS` — the landing zone for every event, valid or invalid, with columns for `event_id`, `event_type`, `user_id`, `hashtags` (VARIANT — Snowflake's native JSON column type), `raw_payload` (full event as VARIANT for audit), `is_valid` (BOOLEAN), and `validation_errors`. `CURATED` holds `USER_DIM` (100 users with type, follower count, country), `POST_DIM` (500 posts with content type and hashtag arrays), and `CURATED_EVENTS` (enriched events joined with both dimensions). `ANALYTICS` holds four output tables: `TRENDING_HASHTAGS`, `VIRAL_POSTS`, `INFLUENCER_RANKING`, and `COMMENT_SENTIMENT` — the tables Tableau connects to.

---

### `simulator/event_simulator.py` — Event Generator

Acts as the "social media application." Generates realistic user activity at 1,000 events per minute and publishes to Kafka. Configuration constants define the simulation bounds: `NUM_USERS=100`, `NUM_POSTS=500`, `NUM_INFLUENCERS=10` (first 10 users get influencer status and higher follower counts), `NUM_VIRAL_POSTS=50` (first 50 posts are picked more often), `VIRAL_BURST_SIZE=600` (exceeds the 500-like viral detection threshold). Static reference data — 30 hashtags, 5 content types, 10 countries, weighted comment pools (70% positive, 15% negative, 15% neutral) — is used to generate realistic-looking events. The event type pool is weighted: LIKE at 35%, VIDEO_VIEW 25%, PROFILE_VISIT 15%, COMMENT 10%, SHARE 7%, FOLLOW 5%, POST_CREATED 3% — mirroring real social platform behaviour. On startup, `populate_dimensions()` seeds `USER_DIM` and `POST_DIM` in Snowflake before streaming begins. `KafkaProducer` includes a graceful fallback: if `kafka-python` is not installed, the simulator prints a warning and continues running without Kafka. The `viral_burst()` function fires 600 LIKE events to one random post in rapid succession — 100 events per second for 6 seconds — to trigger the viral detection pipeline.

---

### `kafka/create_topics.py` — Topic Setup

Creates the Kafka topic before any other component starts. `KafkaAdminClient` connects to `localhost:9092`. `NewTopic('social-events', num_partitions=3)` creates the topic with three partitions — allowing three Spark tasks to read in parallel, one per partition, matching the `spark.sql.shuffle.partitions=4` configuration. `TopicAlreadyExistsError` is silently caught, making the script safe to re-run.

---

### `kafka/validate_consumer.py` — Ingestion Validator

Verifies the pipeline is running correctly by consuming from `social-events` with a separate consumer group (`group_id='validator'`) so that validation reads don't affect the Spark jobs' offsets. `auto_offset_reset='earliest'` reads from the beginning on first run. A `defaultdict` counts events by `event_type` and prints the running distribution every 100 messages, confirming all 7 event types are present in realistic proportions.

---

### `spark/utils.py` — Shared Module

Single source of truth for three things imported by every Spark job. `SNOWFLAKE_CONFIG` loads all six connection parameters from `.env` and is passed to `snowflake.connector.connect(**SNOWFLAKE_CONFIG)`. `EVENT_SCHEMA` is the `StructType` Spark uses when calling `from_json()` on Kafka message bytes — all 11 fields declared with their types, including `ArrayType(StringType())` for hashtags. `nan_to_none(v)` solves a subtle Pandas/Snowflake compatibility issue: when a Spark DataFrame is converted to Pandas via `.toPandas()`, optional float fields with no value become `float('nan')` rather than Python `None`. Since `float('nan')` is truthy in Python (`nan or []` evaluates to `nan`), this would cause hashtag serialisation to break silently and Snowflake to reject `NAN` as a literal. `math.isnan(float(v))` catches this and returns `None`, which serialises correctly as SQL NULL.

---

### `spark/s4_stream_processor.py` — RAW Ingestion + Validation (Steps 4 & 5)

Consumes every event from Kafka and writes it to `RAW.RAW_EVENTS` — the immutable landing zone. Events are read with `readStream.format('kafka').option('subscribe', 'social-events')` and decoded with `from_json(col('value').cast('string'), schema)`. The `validate_row(row)` function checks four conditions: `event_id` must exist, `event_type` must be one of the seven valid types, `user_id` must exist, and events of type LIKE/COMMENT/SHARE/VIDEO_VIEW must carry a `post_id`. Every event — valid or invalid — is written to `RAW_EVENTS` with `is_valid` flag and `validation_errors` string. Invalid events are also saved locally as JSON files in `data/bad_records/` for investigation. The `nan_to_none` guard on the `hashtags` field prevents `json.dumps(nan)` from raising a `ValueError` on events that have no hashtags. Nothing is silently discarded — the raw layer is a complete audit trail.

---

### `spark/s6_enrichment.py` — CURATED Layer (Step 6)

Joins streaming events with dimension tables loaded from Snowflake and writes enriched rows to `CURATED.CURATED_EVENTS`. Dimension data (`USER_DIM` and `POST_DIM`) is loaded from Snowflake into Pandas DataFrames at startup, then converted to Spark DataFrames and cached for the lifetime of the streaming job — reading dimensions on every micro-batch would be prohibitively slow. Each batch is joined against the cached dimensions using Spark's `.join()`. The enriched row adds `username`, `user_type`, `follower_count`, and `content_type` from the dimension tables. Events with no matching `user_id` in `USER_DIM` are dropped — they represent references to users that don't exist in the dimension data.

---

### `spark/s7_trending_hashtags.py` — Trending Hashtags (Step 7)

Computes hashtag frequency across three window sizes simultaneously and writes to `ANALYTICS.TRENDING_HASHTAGS`. Events are filtered to those carrying hashtags, then `explode(col('hashtags'))` converts the array column to one row per hashtag per event — a post with three hashtags produces three rows. The reusable `make_window_agg(df, window_duration)` function builds `groupBy(window(col('ts'), duration), col('hashtag')).agg(count('*').alias('mention_count'))` and tags each row with the window size. Three separate streaming aggregations — `agg_1m`, `agg_5m`, `agg_15m` — are unioned into one stream. A single `writeStream.foreachBatch` sink writes all three granularities to the same Snowflake table in each batch.

---

### `spark/s8_viral_detection.py` — Viral Post Detection (Step 8)

Detects posts that accumulate more than 500 LIKE events within any 5-minute window. Only `event_type == 'LIKE'` events with a non-null `post_id` enter the aggregation: `groupBy(window(col('ts'), '5 minutes'), col('post_id')).agg(count('*').alias('like_count'))`. Crucially, the viral threshold filter happens inside `foreachBatch` rather than before `writeStream` — Spark must track all posts' like counts in window state to know when any of them cross 500; filtering before aggregation would make detection of threshold crossings impossible. `outputMode('update')` re-emits each window row every time the like count changes. The simulator's `viral_burst()` fires 600 likes in ~36 seconds — enough to push one post over the threshold within a single 5-minute window, triggering a `VIRAL DETECTED` console alert and an insert into `ANALYTICS.VIRAL_POSTS`.

---

### `spark/s9_influencer_ranking.py` — Influencer Engagement Scoring (Step 9)

Scores every user's activity in 15-minute windows using a weighted formula. A `when().otherwise()` chain assigns a weight to each event: LIKE=1.0, VIDEO_VIEW=0.5, FOLLOW=2.0, COMMENT=3.0, SHARE=5.0 — comments and shares are weighted higher because they indicate active intent rather than passive consumption. `groupBy(window(col('ts'), '15 minutes'), col('user_id')).agg(_sum('weight').alias('engagement_score'))` produces one row per user per window. In `write_rankings`, Pandas rank is computed with `pdf['engagement_score'].rank(ascending=False, method='min')` and all rows are inserted into `ANALYTICS.INFLUENCER_RANKING` with the top 5 printed to console.

---

### `spark/s10_sentiment.py` — Comment Sentiment Analysis (Step 10)

Classifies every comment's text as POSITIVE, NEUTRAL, or NEGATIVE using keyword matching. `NEGATIVE_KEYWORDS` (15 words including 'terrible', 'awful', 'hate') and `POSITIVE_KEYWORDS` (15 words including 'love', 'amazing', 'incredible') define the vocabulary. `classify_sentiment(text)` lowercases the text and checks negative keywords first — this ordering prevents 'not great' from triggering POSITIVE on the word 'great'. Returns 'NEUTRAL' for null or empty text. `udf(classify_sentiment, StringType())` registers the function as a Spark UDF so it runs in parallel across all executor threads. Only `event_type == 'COMMENT'` events with non-null text enter the stream. Results are written to `ANALYTICS.COMMENT_SENTIMENT` with a per-batch distribution summary printed for monitoring.

---

## For the Room — Plain-Language Walkthrough

---

### Setup (`setup.sql`) — Building the Warehouse Before the Data Arrives

Before a drop of data flows, the filing cabinet needs to be set up. This SQL file creates the entire database structure: three separate storage layers — one for raw data exactly as it arrived, one for cleaned and enriched data, and one for the finished summaries that dashboards read. Creating the structure first means every incoming event has somewhere to go immediately. Think of it like setting up a sorting office with labelled trays before the post van arrives.

### The Simulator — Playing the Role of a Social Media App

We don't have 100,000 real users, so we built a robot to play all of them. The simulator runs continuously, sending 1,000 fake events per minute to Kafka — likes, comments, shares, video views, follows, and the occasional new post. Every few seconds it also picks one post and floods it with 600 likes in rapid succession, deliberately trying to trigger the viral detection system. Without the simulator, the rest of the platform has nothing to process. With it running, the whole system behaves as if it's attached to a live social network.

### Kafka Topic Setup — Preparing the Pipeline

Before any data can flow through Kafka, the topic needs to exist. This script creates `social-events` with three parallel lanes (partitions). Why three? Because we have six Spark jobs consuming the same topic, and distributing the load across multiple lanes means no single lane becomes a bottleneck. It's like opening three checkout queues at a supermarket instead of one.

### Validation Consumer — Checking the Plumbing Works

Before running the full pipeline, this script lets you peek into Kafka and confirm events are arriving in the right shape and the right proportions. It counts events by type and prints the running tally every 100 messages. If LIKEs aren't showing up, or if the ratio looks wrong, you catch it here — before it silently corrupts the downstream analytics.

### Shared Utilities (`utils.py`) — One Place for Shared Settings

All six Spark jobs need the same three things: the Snowflake connection details, the expected shape of an incoming event, and a function to deal with a subtle data conversion bug. Rather than duplicating these across six files, they live here and are imported. One change in one place applies everywhere. The bug it solves is worth explaining: when Spark converts its internal data format to Python's Pandas format, empty number fields don't become "empty" — they become a special float value called `nan` that looks like a number but isn't. Snowflake refuses to store it. The helper function catches this and converts it to a proper null before writing.

### Step 4 — Catching Every Event (Raw Ingestion)

The first Spark job is the gatekeeper. It reads every event from Kafka and asks: is this event valid? Does it have an ID? Is the event type one we recognise? For events involving a post — likes, comments, shares — does it say which post? Events that pass all checks are marked valid. Events that fail are marked invalid, and a copy is saved locally for investigation. But critically — nothing is thrown away. Both valid and invalid events are written to the raw database layer. This is the most important design decision of the whole platform: the raw archive is a complete, unchangeable record of exactly what happened, exactly as it arrived.

### Step 6 — Adding Context to the Events (Enrichment)

A raw event might say "user U0023 liked post P0104." That's useful, but a dashboard wants to know: "A regular user with 450 followers liked a video post." Step 6 does this enrichment. At startup it loads the user table and post table into memory — once, not on every event — then for each incoming event it looks up the user and post details and attaches them. The enriched version, now with usernames, follower counts, and content types, is written to the curated layer. This is the "value-added" version of the data.

### Step 7 — The Trending Hashtags Board

This is the real-time equivalent of Twitter's trending topics. Every event that carries hashtags is processed by this job. A post with three hashtags produces three entries — one per hashtag. Spark counts how many times each hashtag has been used in the last 1 minute, last 5 minutes, and last 15 minutes, simultaneously. All three counts are written to the same Snowflake table. The Tableau dashboard reads this table and shows a constantly updating list of what the platform is talking about right now, at three different levels of recency.

### Step 8 — Spotting the Viral Moment

Every 30 seconds, this job asks: has any post received more than 500 likes in the last 5 minutes? If yes — that post is going viral. The moment the threshold is crossed, an alert fires and the post is recorded in the `VIRAL_POSTS` table. The simulator deliberately triggers this every 60 seconds by sending 600 likes to the same post in quick succession, so you can watch the detection work in real time. The engineering subtlety: the threshold check must happen after counting, not before — you need to watch all posts to know which ones cross 500, not just the ones that already have.

### Step 9 — Ranking the Most Engaged Users

Not all activity is equal. Clicking a like takes one second of thought. Writing a comment takes effort. Sharing something means you're endorsing it publicly. Step 9 scores every user's activity every 15 minutes using a weighted system: likes are worth 1 point, follows 2, comments 3, shares 5. At the end of each 15-minute window, every user has an engagement score and a rank. The result is a leaderboard — who was the most influential user on the platform in the last 15 minutes? The Tableau dashboard shows this updating in near-real-time.

### Step 10 — Reading the Mood

Every comment that comes through is classified: is it positive, negative, or neutral? The classification is done with a keyword list — words like "amazing" and "love" trigger positive, words like "terrible" and "awful" trigger negative, everything else is neutral. The checking order matters: negative keywords are checked first, so a comment like "not great" doesn't accidentally get labelled positive because it contains the word "great." Results flow into a Snowflake table that a dashboard can aggregate — for instance, showing that a particular viral post has an unusually negative comment sentiment, suggesting the virality might be controversy rather than enthusiasm.

---

## How to Run — End to End

### Prerequisites

- Docker and docker-compose installed
- PySpark installed with the Kafka connector JAR (`spark-sql-kafka-0-10_2.12:3.5.3`)
- `.env` file at project root with Snowflake credentials
- Snowflake account with `SOCIAL_MEDIA_DB` accessible (or created by setup.sql below)

---

### Step 1 — Run setup.sql in Snowflake (one time only)

Copy and run `phase5_social_media/snowflake/setup.sql` in the Snowflake web UI.

This creates `SOCIAL_MEDIA_DB` with three schemas (`RAW`, `CURATED`, `ANALYTICS`) and all required tables. Only needs to run once — all `CREATE ... IF NOT EXISTS` statements are safe to re-run if needed.

---

### Step 2 — Start Kafka

```bash
cd phase5_social_media
docker-compose up -d
```

Wait ~15 seconds for Zookeeper (port 2181) and the Kafka broker (port 9092) to be fully ready.

---

### Step 3 — Create the Kafka topic

```bash
python phase5_social_media/kafka/create_topics.py
```

This creates the `social-events` topic with 3 partitions. Must run after Kafka is up.

---

### Step 4 — (Optional) Verify Kafka is receiving events

```bash
python phase5_social_media/kafka/validate_consumer.py
```

Leave this running in a separate terminal to monitor the raw event stream before starting Spark.

---

### Step 5 — Start the event simulator

```bash
python phase5_social_media/simulator/event_simulator.py
```

On startup the simulator seeds `USER_DIM` and `POST_DIM` in Snowflake, then begins publishing ~1,000 events per minute to the `social-events` topic. It fires a viral burst (600 likes to one post) every 60 seconds to trigger the viral detection job. Leave this running for the duration of the demo.

---

### Step 6 — Start all six Spark streaming jobs

Each job runs as a separate long-running process. Open six terminals (or use `&` to background them).

```bash
# Terminal 1 — RAW ingestion: validates every event and writes to RAW.RAW_EVENTS
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase5_social_media/spark/s4_stream_processor.py

# Terminal 2 — CURATED enrichment: joins events with USER_DIM and POST_DIM
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase5_social_media/spark/s6_enrichment.py

# Terminal 3 — Trending hashtags: 1-min, 5-min, 15-min window counts
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase5_social_media/spark/s7_trending_hashtags.py

# Terminal 4 — Viral detection: fires when any post gets >500 likes in 5 minutes
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase5_social_media/spark/s8_viral_detection.py

# Terminal 5 — Influencer ranking: weighted engagement score per user, every 15 minutes
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase5_social_media/spark/s9_influencer_ranking.py

# Terminal 6 — Sentiment: classifies every comment as positive / negative / neutral
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase5_social_media/spark/s10_sentiment.py
```

Start s4 first — the other jobs depend on events being written to `RAW_EVENTS` and `CURATED_EVENTS`.

---

### Step 7 — Connect Tableau

In Tableau Desktop:

1. Connect → Snowflake
2. Enter credentials from `.env` (account, user, password, warehouse)
3. Select database `SOCIAL_MEDIA_DB`, schema `ANALYTICS`
4. Connect to any of the four output tables: `TRENDING_HASHTAGS`, `VIRAL_POSTS`, `INFLUENCER_RANKING`, `COMMENT_SENTIMENT`
5. Set the data source to auto-refresh every 60 seconds to see live updates

---

### What to watch during the demo

| What to open | What you will see |
|---|---|
| Kafka validate_consumer.py | Raw events arriving from the simulator |
| Spark terminal 1 (s4) | Validation pass/fail counts per micro-batch |
| Spark terminal 4 (s8) | Viral alert fired when post crosses 500 likes |
| Snowflake → ANALYTICS.TRENDING_HASHTAGS | Hashtag counts updating every minute |
| Tableau dashboard | Live charts refreshing automatically |

---

### Shutdown

```bash
# Ctrl+C all Spark terminals and the simulator
# Then stop Kafka and Zookeeper
cd phase5_social_media
docker-compose down
```
