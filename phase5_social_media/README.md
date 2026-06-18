# Phase 5 — End-to-End Social Media Analytics Platform

## Architecture Diagram

```text
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│                                                                     │
│   simulator/event_simulator.py                                      │
│   Generates 1,000 events/min                                        │
│   Users: 100 (10 influencers)  Posts: 500 (50 viral candidates)     │
│   Event types: POST_CREATED · LIKE · COMMENT · SHARE               │
│                FOLLOW · VIDEO_VIEW · PROFILE_VISIT                  │
└────────────────────────┬────────────────────────────────────────────┘
                         │ publishes to
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     KAFKA INGESTION LAYER                           │
│                                                                     │
│   Topic: social-events  (3 partitions)                              │
│                                                                     │
│   kafka/create_topics.py    — creates topics                        │
│   kafka/validate_consumer.py — validates ingestion                  │
│   docker-compose.yml        — Zookeeper + Kafka broker              │
└────────────────────────┬────────────────────────────────────────────┘
                         │ consumed by
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  SPARK STRUCTURED STREAMING                         │
│                                                                     │
│  s4_stream_processor.py  — validate → RAW.RAW_EVENTS               │
│  s6_enrichment.py        — join dims → CURATED.CURATED_EVENTS       │
│  s7_trending_hashtags.py — 1/5/15-min windows → TRENDING_HASHTAGS  │
│  s8_viral_detection.py   — >500 likes/5min → VIRAL_POSTS           │
│  s9_influencer_ranking.py — engagement score → INFLUENCER_RANKING  │
│  s10_sentiment.py        — keyword classify → COMMENT_SENTIMENT     │
└────────────────────────┬────────────────────────────────────────────┘
                         │ writes to
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE  (SOCIAL_MEDIA_DB)                     │
│                                                                     │
│  RAW schema                                                         │
│  └── RAW_EVENTS          every event, valid + invalid              │
│                                                                     │
│  CURATED schema                                                     │
│  ├── USER_DIM             100 users with type / follower count      │
│  ├── POST_DIM             500 posts with content type / hashtags    │
│  └── CURATED_EVENTS       enriched events (user + post context)     │
│                                                                     │
│  ANALYTICS schema                                                   │
│  ├── TRENDING_HASHTAGS    top hashtags per 1/5/15-min window        │
│  ├── VIRAL_POSTS          posts that hit >500 likes in 5 min        │
│  ├── INFLUENCER_RANKING   engagement score per user per 15 min      │
│  └── COMMENT_SENTIMENT    POSITIVE / NEUTRAL / NEGATIVE per comment │
└────────────────────────┬────────────────────────────────────────────┘
                         │ visualised in
                         ▼
                    Power BI Dashboards
```

---

## Event Types

| Event Type | Triggered when | Key Fields | Used for |
| --- | --- | --- | --- |
| `POST_CREATED` | A user publishes new content | `post_id`, `content_type`, `hashtags` | Hashtag trending, post dimension |
| `LIKE` | A user likes a post | `user_id`, `post_id` | Viral detection, engagement score ×1 |
| `COMMENT` | A user comments on a post | `user_id`, `post_id`, `comment_text` | Sentiment analysis, engagement score ×3 |
| `SHARE` | A user shares a post | `user_id`, `post_id` | Engagement score ×5 (highest weight — active amplification) |
| `FOLLOW` | A user follows another user | `user_id`, `target_user_id` | Influencer growth tracking, engagement score ×2 |
| `VIDEO_VIEW` | A user watches a video post | `user_id`, `post_id`, `video_duration_sec`, `watch_time_sec` | Engagement score ×0.5, watch-time ratio |
| `PROFILE_VISIT` | A user visits another user's profile | `user_id`, `target_user_id` | Reach / discovery metric |

All 7 event types land in `RAW.RAW_EVENTS`. Only events with a `post_id` go to downstream aggregation tables.

---

## Data Model

```text
CURATED.USER_DIM                    CURATED.POST_DIM
──────────────────                  ──────────────────
user_id  PK                         post_id  PK
username                            user_id  FK → USER_DIM
user_type (regular/influencer)      content_type
follower_count                      hashtags  (VARIANT / JSON array)
following_count                     created_at
post_count
country
created_at
        │                                   │
        └──────────────┬────────────────────┘
                       │ joined into
                       ▼
           CURATED.CURATED_EVENTS  (one row per event)
           ──────────────────────────────────────────
           event_id      event_type     user_id
           username       user_type     post_id
           content_type   hashtags      comment_text
           video_duration_sec           watch_time_sec
           event_timestamp              ingested_at
                       │
                       │ aggregated into
                       ▼
    ┌──────────────────────────────────────────────┐
    │            ANALYTICS schema                  │
    │                                              │
    │  TRENDING_HASHTAGS      VIRAL_POSTS          │
    │  ─────────────────      ───────────          │
    │  hashtag                post_id              │
    │  window_size            window_start/end     │
    │  window_start/end       like_count           │
    │  mention_count          detected_at          │
    │  calculated_at                               │
    │                                              │
    │  INFLUENCER_RANKING     COMMENT_SENTIMENT    │
    │  ──────────────────     ─────────────────    │
    │  user_id                event_id             │
    │  window_start/end       user_id / post_id    │
    │  engagement_score       comment_text         │
    │  like/comment/share/    sentiment            │
    │  video_view/follow cnt  event_timestamp      │
    │  calculated_at          processed_at         │
    └──────────────────────────────────────────────┘
```

Also feeds: `RAW.RAW_EVENTS` — immutable landing zone (every event, valid + invalid, full JSON payload preserved in `raw_payload`).

---

## Business KPIs & Success Criteria

| KPI                  | Definition                                         | Target                                      | Source Table                                |
| -------------------- | -------------------------------------------------- | ------------------------------------------- | ------------------------------------------- |
| Trending hashtags    | Top 10 hashtags by mention count in last 5 min     | Refreshed every 30 sec                      | TRENDING_HASHTAGS                           |
| Viral post detection | Posts with >500 likes in a 5-min window            | Alert within 1 minute of threshold crossing | VIRAL_POSTS                                 |
| Top influencers      | Users ranked by engagement score per 15-min window | Top 10 updated every 30 sec                 | INFLUENCER_RANKING                          |
| Comment sentiment    | % Positive / Neutral / Negative comments           | >60% Positive baseline                      | COMMENT_SENTIMENT                           |
| Data quality         | % of events passing validation                     | >99% valid events                           | RAW_EVENTS (is_valid)                       |
| Ingestion latency    | Time from event creation to Snowflake write        | <30 seconds end-to-end                      | RAW_EVENTS (event_timestamp vs ingested_at) |

---

## Engagement Scoring Formula

```text
engagement_score = likes × 1
                 + comments × 3
                 + shares × 5
                 + video_views × 0.5
                 + follows × 2
```

Comments and shares are weighted higher because they indicate active intent, not just passive scrolling.

---

## Project Plan (Steps 1–11)

| Step | Name                      | Key Files                                                                    | Status |
| ---- | ------------------------- | ---------------------------------------------------------------------------- | ------ |
| 1    | Planning & Architecture   | `README.md`, `snowflake/setup.sql`                                           | Done   |
| 2    | Snowflake Setup           | `snowflake/setup.sql`                                                        | Done   |
| 3    | Kafka Ingestion           | `kafka/create_topics.py`, `kafka/validate_consumer.py`, `docker-compose.yml` | Done   |
| 4    | Stream Processing (RAW)   | `spark/s4_stream_processor.py`                                               | Done   |
| 5    | Data Validation           | inside `s4_stream_processor.py`                                              | Done   |
| 6    | Data Enrichment (CURATED) | `spark/s6_enrichment.py`                                                     | Done   |
| 7    | Trending Hashtags         | `spark/s7_trending_hashtags.py`                                              | Done   |
| 8    | Viral Detection           | `spark/s8_viral_detection.py`                                                | Done   |
| 9    | Influencer Ranking        | `spark/s9_influencer_ranking.py`                                             | Done   |
| 10   | Sentiment Analysis        | `spark/s10_sentiment.py`                                                     | Done   |
| 11   | Dashboards                | Power BI (connect to Snowflake ANALYTICS schema)                             | —      |

---

## How to Run

```bash
# 1. Snowflake — run setup.sql in your Snowflake worksheet (one time)

# 2. Kafka
docker-compose up -d
python3 kafka/create_topics.py

# 3. Simulator (Terminal 1)
python3 simulator/event_simulator.py

# 4. Spark jobs — one terminal each
python3 spark/s4_stream_processor.py
python3 spark/s6_enrichment.py
python3 spark/s7_trending_hashtags.py
python3 spark/s8_viral_detection.py
python3 spark/s9_influencer_ranking.py
python3 spark/s10_sentiment.py

# 5. Validate Kafka (optional)
python3 kafka/validate_consumer.py
```

---

## Topic Design

| Topic           | Partitions | Producer             | Consumer       | Purpose                                                               |
| --------------- | ---------- | -------------------- | -------------- | --------------------------------------------------------------------- |
| `social-events` | 3          | `event_simulator.py` | All Spark jobs | Main event stream — 3 partitions = 3 Spark tasks can read in parallel |
