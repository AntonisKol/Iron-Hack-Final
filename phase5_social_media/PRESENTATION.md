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

**Purpose:** One-time setup. Creates the entire Snowflake data warehouse structure before any data flows.

- **Database block:** `CREATE DATABASE IF NOT EXISTS SOCIAL_MEDIA_DB`.
- **RAW schema block:** Creates `RAW.RAW_EVENTS` — the landing zone for every event that arrives, valid or invalid. Columns: `event_id`, `event_type`, `user_id`, `post_id`, `target_user_id`, `hashtags` (VARIANT — Snowflake's JSON column type), `comment_text`, `raw_payload` (full JSON as VARIANT), `is_valid` (BOOLEAN), `event_timestamp`, `ingested_at`.
- **CURATED schema block:** Creates `CURATED.USER_DIM` (100 users with type, follower count, country), `CURATED.POST_DIM` (500 posts with content type and hashtags), `CURATED.CURATED_EVENTS` (enriched events joined with both dimensions).
- **ANALYTICS schema block:** Creates `ANALYTICS.TRENDING_HASHTAGS`, `ANALYTICS.VIRAL_POSTS`, `ANALYTICS.INFLUENCER_RANKING`, `ANALYTICS.COMMENT_SENTIMENT` — the four insight tables that Tableau reads.

---

### `simulator/event_simulator.py` — Event Generator

**Purpose:** Act as the "social media application." Generates realistic user activity at 1,000 events/minute and publishes to Kafka. Also seeds the dimension tables.

- **CONFIGURATION block:** Constants — `NUM_USERS=100`, `NUM_POSTS=500`, `NUM_INFLUENCERS=10` (first 10 users), `NUM_VIRAL_POSTS=50` (first 50 posts), `EVENTS_PER_MINUTE=1000`, `SLEEP_INTERVAL=0.06s`, `VIRAL_BURST_SIZE=600` (exceeds the viral detection threshold of 500).
- **Static reference data block:** `HASHTAG_POOL` (30 hashtags), `CONTENT_TYPES`, `COUNTRIES`, pre-written positive/negative comment strings. Used to generate realistic-looking events without needing a real database.
- **KafkaProducer init block:** Graceful fallback — if `kafka-python` is not installed, the simulator still writes events to a local folder. When Kafka is available, it serialises each event dict as UTF-8 JSON and sends to `social-events` topic.
- **Snowflake seed block:** On startup, populates `USER_DIM` and `POST_DIM` with the reference users and posts. This ensures the enrichment job (`s6`) has dimension data to join against.
- **Event generation loop:** Infinite loop. Each iteration picks a random event type from a weighted distribution (LIKEs are most common, POSTs least common — mirrors real social media behaviour). Calls the appropriate generator function, publishes to Kafka, sleeps 0.06 seconds.
- **`viral_burst()` function:** Fires 600 LIKE events to one random post in rapid succession — designed to exceed the 500-likes-per-5-minutes threshold that `s8_viral_detection.py` watches for.

---

### `kafka/create_topics.py` — Topic Setup

**Purpose:** Create the Kafka topic before the simulator or any Spark job starts.

- **`KafkaAdminClient` block:** Connects to `localhost:9092`.
- **`NewTopic` block:** `social-events` with `num_partitions=3`. Three partitions allows three Spark tasks to read in parallel — one task per partition — matching the default `spark.sql.shuffle.partitions=4` configuration.
- **Error handling:** `TopicAlreadyExistsError` is silently caught — safe to re-run.

---

### `kafka/validate_consumer.py` — Ingestion Validator

**Purpose:** Verify the pipeline is running correctly. Connects to `social-events` and prints a live event-type count summary.

- **Consumer block:** `auto_offset_reset='earliest'` — reads from the beginning on first run. `group_id='validator'` — separate consumer group from the Spark jobs, so validation doesn't affect their offsets.
- **Counting loop:** Maintains a `defaultdict` counting events per `event_type`. Every 100 messages, prints the running distribution. Confirms all 7 event types are present and the ratio looks realistic.

---

### `spark/utils.py` — Shared Module

**Purpose:** Single source of truth for three things used by all six Spark jobs. Imported via `from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema, nan_to_none`.

- **`SNOWFLAKE_CONFIG` dict:** Account, user, password, database, warehouse — all loaded from `.env`. Every Spark job passes this dict to `snowflake.connector.connect(**SNOWFLAKE_CONFIG)`.
- **`EVENT_SCHEMA` StructType:** Defines the JSON structure that Spark expects when parsing Kafka messages with `from_json()`. All 11 fields declared with their Spark types. Using `ArrayType(StringType())` for `hashtags` — Snowflake's VARIANT becomes a Python list.
- **`nan_to_none(v)` function:** Solves a subtle bug. When a Spark DataFrame is converted to Pandas via `.toPandas()`, optional float fields that have no value become `float('nan')` — not Python `None`. But `float('nan')` is **truthy** in Python (`nan or []` returns `nan`, not `[]`), and Snowflake rejects `NAN` as a literal. `nan_to_none` uses `math.isnan(float(v))` to catch this and return `None` instead.

---

### `spark/s4_stream_processor.py` — RAW Ingestion + Validation (Steps 4 & 5)

**Purpose:** Consume every event from Kafka, validate it, and write to `RAW.RAW_EVENTS` — the immutable landing zone.

- **SparkSession block:** `spark.jars.packages` pulls the Kafka connector JAR. `spark.sql.shuffle.partitions=4` limits shuffle overhead for local execution.
- **READ FROM KAFKA block:** `readStream.format('kafka').option('subscribe', 'social-events').load()`. `.select(from_json(col('value').cast('string'), schema).alias('d')).select('d.*')` — deserialises the JSON bytes into a typed DataFrame.
- **`write_to_raw(batch_df, batch_id)` function (foreachBatch):** Called every 15 seconds with the latest micro-batch. For each row: validates `event_type` is in `VALID_EVENT_TYPES`, validates `user_id` and `event_id` are not null, sets `is_valid` flag. Writes every event (valid AND invalid) to `RAW_EVENTS` — the raw layer never drops records. Invalid events are preserved for audit and debugging.
- **`nan_to_none` guard on `hashtags`:** The `hashtags` column can be `float('nan')` if the event type doesn't carry hashtags. Without the guard, `json.dumps(nan)` raises `ValueError`. The guard returns `None` → serialised as `null` → Snowflake inserts `NULL`.

---

### `spark/s6_enrichment.py` — CURATED Layer (Step 6)

**Purpose:** Join streaming events with dimension tables loaded from Snowflake, write enriched rows to `CURATED.CURATED_EVENTS`.

- **Dimension loading block (at startup):** Reads `USER_DIM` and `POST_DIM` from Snowflake into Pandas DataFrames, converts to Spark DataFrames. These are loaded once and cached for the lifetime of the streaming job — reading dimensions from Snowflake on every micro-batch would be too slow.
- **READ FROM KAFKA block:** Same pattern as `s4`.
- **`enrich_and_write(batch_df, batch_id)` function:** Joins each batch against the cached dimension DataFrames using Spark's `.join()`. Adds `username`, `user_type`, `follower_count`, `content_type` from the dimension tables. Writes only enriched (successfully joined) rows to `CURATED_EVENTS`. Events with no matching `user_id` in `USER_DIM` are silently dropped — they're invalid references.

---

### `spark/s7_trending_hashtags.py` — Trending Hashtags (Step 7)

**Purpose:** Compute hashtag frequency across three window sizes simultaneously and write to `ANALYTICS.TRENDING_HASHTAGS`.

- **READ + WATERMARK block:** `withWatermark('ts', '5 minutes')` — accepts late events up to 5 minutes old.
- **Hashtag explosion block:** `filter(col('hashtags').isNotNull())` → `.select(col('ts'), explode(col('hashtags')).alias('hashtag'))`. `explode()` takes an array column and creates one row per array element — a post with three hashtags produces three rows.
- **`make_window_agg(df, window_duration)` function:** Reusable helper. Builds `groupBy(window(col('ts'), duration), col('hashtag')).agg(count('*').alias('mention_count'))` and tags the result with the window size string.
- **Three aggregations + union:** `agg_1m`, `agg_5m`, `agg_15m` — three separate streaming aggregations on the same DataFrame. `.union()` merges all three into one stream. A single `writeStream.foreachBatch(write_trending)` writes all three granularities to the same Snowflake table in one batch.

---

### `spark/s8_viral_detection.py` — Viral Post Detection (Step 8)

**Purpose:** Detect posts that accumulate more than 500 LIKE events within any 5-minute window.

- **`VIRAL_THRESHOLD = 500` constant:** Configurable — can be tuned without touching the logic.
- **Aggregation block:** Filters only `event_type == 'LIKE'` and `post_id IS NOT NULL`. `groupBy(window(col('ts'), '5 minutes'), col('post_id')).agg(count('*').alias('like_count'))`. Note: the filter for viral threshold happens **inside `foreachBatch`**, not before `writeStream`. This is intentional — Spark must track all posts' like counts in window state to know when any of them cross 500. Filtering before aggregation would only track already-viral posts, making detection impossible.
- **`detect_and_write(batch_df, batch_id)` function:** Applies `filter(col('like_count') >= VIRAL_THRESHOLD)` on the batch. If any post crossed the threshold: prints a `VIRAL DETECTED` alert to console, inserts into `ANALYTICS.VIRAL_POSTS`.
- **`outputMode('update')`:** Re-emits each window row every time the like count changes. The simulator's `viral_burst()` function fires 600 likes in ~36 seconds — enough to push one post over the threshold within a single 5-minute window.

---

### `spark/s9_influencer_ranking.py` — Influencer Engagement Scoring (Step 9)

**Purpose:** Score every user's activity in 15-minute windows using a weighted formula and rank them.

- **Weight assignment block:** `when(col('event_type') == 'LIKE', lit(1.0)).when(...COMMENT..., lit(3.0)).when(...SHARE..., lit(5.0)).when(...VIDEO_VIEW..., lit(0.5)).when(...FOLLOW..., lit(2.0)).otherwise(lit(0.0))` — adds a `weight` column to each event row. Comments and shares are weighted higher because they indicate intent, not passive consumption.
- **Aggregation block:** `groupBy(window(col('ts'), '15 minutes'), col('user_id')).agg(_sum('weight').alias('engagement_score'), count(when(event_type == 'LIKE', True)).alias('like_count'), ...)` — one row per user per 15-minute window with total score and per-type breakdown.
- **`write_rankings(batch_df, batch_id)` function:** Computes batch-level rank with `pdf['engagement_score'].rank(ascending=False, method='min')`. Inserts all rows into `ANALYTICS.INFLUENCER_RANKING`. Prints top 5 for monitoring.

---

### `spark/s10_sentiment.py` — Comment Sentiment Analysis (Step 10)

**Purpose:** Classify every comment's text as POSITIVE, NEUTRAL, or NEGATIVE using keyword matching. Write results to `ANALYTICS.COMMENT_SENTIMENT`.

- **Keyword lists block:** `NEGATIVE_KEYWORDS` (15 words: 'terrible', 'awful', 'hate', etc.) and `POSITIVE_KEYWORDS` (15 words: 'love', 'amazing', 'incredible', etc.). Negative is checked first — if a comment contains 'not great', checking negative first avoids the word 'great' triggering POSITIVE.
- **`classify_sentiment(text)` function:** Lowercases the text, iterates negative keywords first, then positive. Returns 'NEGATIVE', 'POSITIVE', or 'NEUTRAL'. Returns 'NEUTRAL' for null or empty text.
- **`sentiment_udf = udf(classify_sentiment, StringType())`:** Registers the function as a Spark UDF — Spark distributes it across all executor threads. `StringType()` tells Spark the return type so it can build the execution plan.
- **Stream filter block:** `filter(col('event_type') == 'COMMENT')` and `filter(col('comment_text').isNotNull())` — only COMMENT events with non-null text enter the aggregation.
- **`write_sentiment(batch_df, batch_id)` function:** Inserts rows into `ANALYTICS.COMMENT_SENTIMENT`. Prints a distribution summary: how many POSITIVE / NEUTRAL / NEGATIVE comments in this batch.

---

## For the Room (Non-Technical)

Imagine Instagram, TikTok, or Twitter. Every time you tap a heart, leave a comment, or share a post — something happens on the other side of the screen. A message gets sent to a computer saying "user 42 just liked post 99." In this phase, we built everything that happens after that tap — the entire invisible machinery.

---

**The simulator is the social media app.**

We don't have real users, so we built a robot that pretends to be 100 of them. The robot runs non-stop, sending 1,000 fake events per minute — likes, comments, shares, video views, follows. Once in a while, it makes one post go viral by sending 600 likes in 36 seconds. That triggers our detection system.

---

**Kafka is the pipeline.**

Think of it as a very fast motorway with three lanes. Every event (like, comment, share) gets on the motorway. Six different destinations (our Spark jobs) are all watching the motorway simultaneously — each one picks up the events it cares about. The motorway never gets full, never drops messages, and if one destination goes offline, the messages wait for it.

---

**The six Spark jobs are the analysts.**

Each one is watching the Kafka motorway for something specific:

- **Job 1 (s4):** Catches every single event and stores it in a "raw archive." Nothing is thrown away.
- **Job 2 (s6):** Takes each event and looks up extra information — "this like came from user 42, who has 12,000 followers and is from Brazil." Stores the enriched version.
- **Job 3 (s7):** Counts hashtags. Every minute it knows the top trending hashtags — like Twitter's trending list, but rebuilt from scratch.
- **Job 4 (s8):** Watches for posts going viral. The moment any post gets more than 500 likes in 5 minutes, it fires an alert.
- **Job 5 (s9):** Scores every user based on how engaging their activity is. Liking something is worth 1 point. Commenting is worth 3. Sharing is worth 5. Every 15 minutes it publishes a leaderboard.
- **Job 6 (s10):** Reads every comment and decides if it's positive, neutral, or negative — like a mood detector for the platform.

---

**Snowflake is the organised filing cabinet.**

Three drawers:
1. **RAW** — everything, as it arrived, no changes. Like a security camera recording. If anything goes wrong, you can go back and see exactly what happened.
2. **CURATED** — clean, enriched data. Like the security footage after someone has tagged each person in it.
3. **ANALYTICS** — the summaries. The four final tables that answer the actual business questions: what's trending? which post went viral? who's the top influencer this hour? what's the mood?

---

**Tableau is the window into all of this.**

The dashboards connect directly to the ANALYTICS tables. Every time a manager refreshes the screen, they're seeing data that's at most 30 seconds old. Not a report someone made yesterday — live data, right now.

---

**The headline of this phase:**

We built in one laptop what a social media company's data team operates in a server room. The concepts, the architecture, and the code patterns are identical — ours just run locally instead of on a hundred servers. If you replaced `localhost:9092` with a real Kafka cluster address, and `local[*]` with a real Spark cluster, this would be production-ready.
