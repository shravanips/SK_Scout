"""
scrape_posts.py — Fetch all posts + engagement metrics for each celebrity
                  from 2020-01-01 to today.

Uses Twitter API v2 timelines endpoint with full-archive search
(requires Academic/Pro tier for pre-recent data).

Usage:
    python scrape_posts.py
    python scrape_posts.py --username KimKardashian
    python scrape_posts.py --config config/config.yaml
"""

import argparse
import json
import sqlite3
from datetime import datetime, timezone

import tweepy
from tqdm import tqdm

from db import get_conn, init_db
from utils import (
    EXPANSIONS, MEDIA_FIELDS, TWEET_FIELDS, USER_FIELDS,
    flatten_tweet, get_logger, load_config, make_client,
    now_iso, parse_twitter_date, safe_request
)

logger = get_logger("post_scraper")

# All fields we want from the API
POST_TWEET_FIELDS = [
    "id", "text", "author_id", "created_at", "lang",
    "public_metrics", "referenced_tweets", "source",
    "attachments", "entities", "in_reply_to_user_id",
    "conversation_id",
]


def get_user_id(conn: sqlite3.Connection, username: str) -> tuple[str | None, int]:
    """Look up user_id and followers_count from the profiles table."""
    row = conn.execute(
        "SELECT user_id, followers_count FROM profiles WHERE LOWER(username) = LOWER(?)",
        (username,)
    ).fetchone()
    if row:
        return row["user_id"], row["followers_count"]
    return None, 0


def fetch_timeline(
    client: tweepy.Client,
    user_id: str,
    start_time: str,
    end_time: str | None,
    max_results: int,
) -> list[dict]:
    """
    Paginate through a user's tweet timeline and return all tweets
    between start_time and end_time.
    """
    all_tweets = []
    paginator = tweepy.Paginator(
        client.get_users_tweets,
        id=user_id,
        tweet_fields=POST_TWEET_FIELDS,
        user_fields=USER_FIELDS,
        media_fields=MEDIA_FIELDS,
        expansions=EXPANSIONS,
        start_time=start_time,
        end_time=end_time,
        max_results=100,   # max per page
        limit=max_results // 100 + 1,
    )

    media_map = {}
    try:
        for page in paginator:
            if not page or not page.get("data"):
                continue

            # Build media lookup for this page
            includes = page.get("includes", {})
            for m in includes.get("media", []):
                media_map[m["media_key"]] = m

            all_tweets.extend(page["data"])

            if len(all_tweets) >= max_results:
                break

    except tweepy.TooManyRequests:
        logger.warning("Rate limit hit during timeline fetch — Tweepy will retry")
    except tweepy.Forbidden as e:
        logger.error(f"Forbidden (check API tier / user suspension): {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching timeline for {user_id}: {e}")

    return all_tweets, media_map


def upsert_posts(
    conn: sqlite3.Connection,
    tweets: list[dict],
    author_followers: int,
    media_map: dict,
    scraped_at: str,
) -> int:
    rows = [flatten_tweet(t, author_followers, media_map, scraped_at) for t in tweets]

    conn.executemany("""
        INSERT INTO posts (
            tweet_id, user_id, text, lang, created_at,
            like_count, retweet_count, reply_count, quote_count,
            bookmark_count, impression_count,
            is_retweet, is_quote, is_reply, referenced_tweet_id,
            source, has_media, media_types, hashtags, mentions,
            urls_count, engagement_rate, scraped_at, raw_json
        ) VALUES (
            :tweet_id, :user_id, :text, :lang, :created_at,
            :like_count, :retweet_count, :reply_count, :quote_count,
            :bookmark_count, :impression_count,
            :is_retweet, :is_quote, :is_reply, :referenced_tweet_id,
            :source, :has_media, :media_types, :hashtags, :mentions,
            :urls_count, :engagement_rate, :scraped_at, :raw_json
        )
        ON CONFLICT(tweet_id) DO UPDATE SET
            like_count       = excluded.like_count,
            retweet_count    = excluded.retweet_count,
            reply_count      = excluded.reply_count,
            quote_count      = excluded.quote_count,
            bookmark_count   = excluded.bookmark_count,
            impression_count = excluded.impression_count,
            engagement_rate  = excluded.engagement_rate,
            scraped_at       = excluded.scraped_at
    """, rows)
    conn.commit()
    return len(rows)


def run_user(
    client: tweepy.Client,
    conn: sqlite3.Connection,
    username: str,
    cfg: dict,
) -> int:
    """Scrape all posts for a single user. Returns number of posts saved."""
    user_id, followers = get_user_id(conn, username)

    if not user_id:
        logger.warning(f"  User '{username}' not found in profiles table. Run scrape_profiles.py first.")
        return 0

    start_time = cfg["collection"]["start_date"] + "T00:00:00Z"
    end_time = cfg["collection"].get("end_date")
    if end_time:
        end_time = end_time + "T23:59:59Z"

    max_posts = cfg["collection"]["max_posts_per_user"]
    logger.info(f"  Fetching posts for @{username} (id={user_id}, followers={followers:,}) ...")

    tweets, media_map = fetch_timeline(client, user_id, start_time, end_time, max_posts)
    logger.info(f"  - Retrieved {len(tweets)} tweets")

    if not tweets:
        return 0

    saved = upsert_posts(conn, tweets, followers, media_map, now_iso())
    logger.info(f"  - Saved {saved} posts to DB")
    return saved


def run(config_path: str = "config/config.yaml", username_filter: str | None = None) -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])
    client = make_client(cfg)

    # Resolve targets
    targets = cfg.get("targets", {})
    all_targets = []
    for cat_list in targets.values():
        all_targets.extend(cat_list)

    if username_filter:
        all_targets = [t for t in all_targets if t["username"].lower() == username_filter.lower()]
        if not all_targets:
            logger.error(f"Username '{username_filter}' not found in config targets")
            return

    total_posts = 0
    for target in tqdm(all_targets, desc="Scraping posts", unit="user"):
        username = target["username"]
        logger.info(f"\n{'-'*60}\n@{username}")
        try:
            count = run_user(client, conn, username, cfg)
            total_posts += count
        except Exception as e:
            logger.error(f"Failed for @{username}: {e}")

    logger.info(f"\n- Post scrape complete. Total posts saved: {total_posts:,}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape celebrity posts from X")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--username", default=None, help="Scrape only this username")
    args = parser.parse_args()
    run(args.config, args.username)
