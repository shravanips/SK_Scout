"""
db.py — SQLite schema + helper functions for the X pipeline.
All tables are created here. Other modules import `get_conn()`.
"""

import sqlite3
import os
from pathlib import Path


# -- Schema DDL ----------------------------------------------------------------

SCHEMA = """
-- ---------------------------------------------
--  PROFILES
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS profiles (
    user_id             TEXT PRIMARY KEY,
    username            TEXT NOT NULL,
    display_name        TEXT,
    profession_category TEXT,          -- actor / singer / reality_tv / athlete / etc
    description         TEXT,
    location            TEXT,
    url                 TEXT,
    profile_image_url   TEXT,
    is_verified         INTEGER DEFAULT 0,
    is_blue_verified    INTEGER DEFAULT 0,
    followers_count     INTEGER,
    following_count     INTEGER,
    tweet_count         INTEGER,
    listed_count        INTEGER,
    account_created_at  TEXT,
    withheld_countries  TEXT,
    scraped_at          TEXT NOT NULL,
    raw_json            TEXT           -- full API response blob
);

-- ---------------------------------------------
--  PROFILE SNAPSHOTS  (track changes over time)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS profile_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id             TEXT NOT NULL,
    followers_count     INTEGER,
    following_count     INTEGER,
    tweet_count         INTEGER,
    snapshot_at         TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES profiles(user_id)
);

-- ---------------------------------------------
--  POSTS / TWEETS
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS posts (
    tweet_id            TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    text                TEXT,
    lang                TEXT,
    created_at          TEXT NOT NULL,
    -- Engagement metrics
    like_count          INTEGER DEFAULT 0,
    retweet_count       INTEGER DEFAULT 0,
    reply_count         INTEGER DEFAULT 0,
    quote_count         INTEGER DEFAULT 0,
    bookmark_count      INTEGER DEFAULT 0,
    impression_count    INTEGER DEFAULT 0,
    -- Metadata
    is_retweet          INTEGER DEFAULT 0,
    is_quote            INTEGER DEFAULT 0,
    is_reply            INTEGER DEFAULT 0,
    referenced_tweet_id TEXT,
    source              TEXT,           -- app used to post
    has_media           INTEGER DEFAULT 0,
    media_types         TEXT,           -- comma-separated: photo,video,gif
    hashtags            TEXT,           -- comma-separated
    mentions            TEXT,           -- comma-separated user_ids
    urls_count          INTEGER DEFAULT 0,
    -- Computed
    engagement_rate     REAL,           -- (likes+rt+replies+quotes) / author_followers
    scraped_at          TEXT NOT NULL,
    raw_json            TEXT
);

CREATE INDEX IF NOT EXISTS idx_posts_user_created
    ON posts(user_id, created_at);

CREATE INDEX IF NOT EXISTS idx_posts_created
    ON posts(created_at);

-- ---------------------------------------------
--  POST ENGAGEMENT SNAPSHOTS (track virality curve)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS post_engagement_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tweet_id        TEXT NOT NULL,
    like_count      INTEGER,
    retweet_count   INTEGER,
    reply_count     INTEGER,
    quote_count     INTEGER,
    snapshot_at     TEXT NOT NULL,
    FOREIGN KEY (tweet_id) REFERENCES posts(tweet_id)
);

-- ---------------------------------------------
--  REPLIES / COMMENTS
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS replies (
    reply_id            TEXT PRIMARY KEY,
    parent_tweet_id     TEXT NOT NULL,
    author_id           TEXT NOT NULL,
    author_username     TEXT,
    text                TEXT,
    lang                TEXT,
    created_at          TEXT NOT NULL,
    like_count          INTEGER DEFAULT 0,
    reply_count         INTEGER DEFAULT 0,
    retweet_count       INTEGER DEFAULT 0,
    -- author metadata at time of reply
    author_followers    INTEGER,
    author_following    INTEGER,
    author_tweet_count  INTEGER,
    author_created_at   TEXT,
    author_verified     INTEGER DEFAULT 0,
    author_has_pfp      INTEGER DEFAULT 1,
    author_has_bio      INTEGER DEFAULT 1,
    -- bot signals
    bot_score           REAL,
    bot_signals         TEXT,           -- JSON array of triggered signals
    scraped_at          TEXT NOT NULL,
    raw_json            TEXT,
    FOREIGN KEY (parent_tweet_id) REFERENCES posts(tweet_id)
);

CREATE INDEX IF NOT EXISTS idx_replies_parent
    ON replies(parent_tweet_id);

CREATE INDEX IF NOT EXISTS idx_replies_author
    ON replies(author_id);

-- ---------------------------------------------
--  FOLLOWER SAMPLES  (sampled, not full list)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS follower_samples (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    target_user_id      TEXT NOT NULL,
    follower_id         TEXT NOT NULL,
    follower_username   TEXT,
    follower_created_at TEXT,
    followers_count     INTEGER,
    following_count     INTEGER,
    tweet_count         INTEGER,
    has_pfp             INTEGER DEFAULT 1,
    has_bio             INTEGER DEFAULT 1,
    is_verified         INTEGER DEFAULT 0,
    bot_score           REAL,
    bot_signals         TEXT,
    sampled_at          TEXT NOT NULL,
    raw_json            TEXT,
    UNIQUE(target_user_id, follower_id)
);

CREATE INDEX IF NOT EXISTS idx_follower_samples_target
    ON follower_samples(target_user_id);

-- ---------------------------------------------
--  TRENDING TOPICS
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS trending_topics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    woeid           INTEGER NOT NULL,
    location_name   TEXT,
    trend_name      TEXT NOT NULL,
    query           TEXT,
    tweet_volume    INTEGER,
    rank            INTEGER,
    captured_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trending_name
    ON trending_topics(trend_name, captured_at);

-- ---------------------------------------------
--  TRENDING TOPIC ANALYSIS
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS trending_analysis (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    trend_name              TEXT NOT NULL,
    period_start            TEXT NOT NULL,
    period_end              TEXT NOT NULL,
    times_trended           INTEGER,
    peak_volume             INTEGER,
    avg_volume              INTEGER,
    celebrity_mentions      TEXT,           -- JSON: {username: count}
    authenticity_score      REAL,           -- 0–1, higher = more authentic
    bot_engagement_ratio    REAL,           -- estimated % bot-driven
    verdict                 TEXT,           -- authentic / suspicious / likely_paid
    evidence                TEXT,           -- JSON array of signals
    analyzed_at             TEXT NOT NULL
);

-- ---------------------------------------------
--  BOT ANALYSIS RESULTS (per profile)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS bot_analysis (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id                     TEXT NOT NULL,
    analysis_date               TEXT NOT NULL,
    total_posts_analyzed        INTEGER,
    total_replies_analyzed      INTEGER,
    total_followers_sampled     INTEGER,
    -- Follower bot estimates
    estimated_bot_followers_pct REAL,
    follower_bot_score          REAL,
    -- Engagement bot estimates
    avg_post_bot_engagement_pct REAL,
    engagement_bot_score        REAL,
    -- Comment bot estimates
    comment_bot_pct             REAL,
    duplicate_comment_clusters  INTEGER,
    -- Overall
    overall_bot_score           REAL,       -- 0–1
    overall_verdict             TEXT,       -- clean / moderate / high_bot / severe
    key_signals                 TEXT,       -- JSON array
    FOREIGN KEY (user_id) REFERENCES profiles(user_id)
);

-- ---------------------------------------------
--  PIPELINE RUN LOG
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    module          TEXT NOT NULL,
    target          TEXT,
    status          TEXT NOT NULL,      -- running / success / failed
    records_written INTEGER DEFAULT 0,
    error_message   TEXT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT
);
"""


def get_conn(db_path: str) -> sqlite3.Connection:
    """Return a SQLite connection with WAL mode and row_factory set."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    """Create all tables if they don't exist."""
    conn = get_conn(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"[DB] Initialised at {db_path}")


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/x_pipeline.db"
    init_db(path)
