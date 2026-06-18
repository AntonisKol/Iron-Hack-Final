CREATE DATABASE IF NOT EXISTS SOCIAL_MEDIA_DB;
USE DATABASE SOCIAL_MEDIA_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE TABLE IF NOT EXISTS RAW.RAW_EVENTS (
    event_id           VARCHAR,
    event_type         VARCHAR,
    user_id            VARCHAR,
    post_id            VARCHAR,
    target_user_id     VARCHAR,
    hashtags           VARIANT,
    comment_text       VARCHAR,
    content_type       VARCHAR,
    video_duration_sec FLOAT,
    watch_time_sec     FLOAT,
    raw_payload        VARIANT,
    event_timestamp    TIMESTAMP,
    ingested_at        TIMESTAMP,
    is_valid           BOOLEAN,
    validation_errors  VARCHAR
);

CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE TABLE IF NOT EXISTS CURATED.USER_DIM (
    user_id         VARCHAR PRIMARY KEY,
    username        VARCHAR,
    user_type       VARCHAR,
    follower_count  INTEGER,
    following_count INTEGER,
    post_count      INTEGER,
    country         VARCHAR,
    created_at      TIMESTAMP
);
CREATE TABLE IF NOT EXISTS CURATED.POST_DIM (
    post_id      VARCHAR PRIMARY KEY,
    user_id      VARCHAR,
    content_type VARCHAR,
    hashtags     VARIANT,
    created_at   TIMESTAMP
);
CREATE TABLE IF NOT EXISTS CURATED.CURATED_EVENTS (
    event_id           VARCHAR,
    event_type         VARCHAR,
    user_id            VARCHAR,
    username           VARCHAR,
    user_type          VARCHAR,
    post_id            VARCHAR,
    content_type       VARCHAR,
    hashtags           VARIANT,
    comment_text       VARCHAR,
    video_duration_sec FLOAT,
    watch_time_sec     FLOAT,
    event_timestamp    TIMESTAMP,
    ingested_at        TIMESTAMP
);

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE TABLE IF NOT EXISTS ANALYTICS.TRENDING_HASHTAGS (
    hashtag       VARCHAR,
    window_size   VARCHAR,
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    mention_count INTEGER,
    calculated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ANALYTICS.VIRAL_POSTS (
    post_id      VARCHAR,
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    like_count   INTEGER,
    detected_at  TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ANALYTICS.INFLUENCER_RANKING (
    user_id          VARCHAR,
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    engagement_score FLOAT,
    like_count       INTEGER,
    comment_count    INTEGER,
    share_count      INTEGER,
    video_view_count INTEGER,
    follow_count     INTEGER,
    rank             INTEGER,
    calculated_at    TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ANALYTICS.COMMENT_SENTIMENT (
    event_id        VARCHAR,
    user_id         VARCHAR,
    post_id         VARCHAR,
    comment_text    VARCHAR,
    sentiment       VARCHAR,
    event_timestamp TIMESTAMP,
    processed_at    TIMESTAMP
);

SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA CURATED;
SHOW TABLES IN SCHEMA ANALYTICS;
