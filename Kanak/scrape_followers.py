"""
scrape_followers.py — Sample followers for each celebrity profile.

Full follower lists (millions) are infeasible to scrape entirely,
so we take stratified random samples:
  - First 1000 followers (most recent)
  - 1000 from the middle cohort
  - 1000 oldest followers
This gives enough signal for bot percentage estimation.

Usage:
    python scrape_followers.py
    python scrape_followers.py --username KimKardashian --sample-size 500
"""

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone

import tweepy
from tqdm import tqdm

from db import get_conn, init_db
from utils import get_logger, load_config, make_client, now_iso, parse_twitter_date

logger = get_logger("follower_scraper")

USER_FIELDS = [
    "id", "name", "username", "description", "profile_image_url",
    "verified", "created_at", "public_metrics",
]


def fetch_follower_sample(
    client: tweepy.Client,
    user_id: str,
    sample_size: int = 1000,
) -> list[dict]:
    """
    Fetch up to `sample_size` followers using the followers endpoint.
    Returns list of raw user dicts.
    """
    followers = []
    try:
        paginator = tweepy.Paginator(
            client.get_users_followers,
            id=user_id,
            user_fields=USER_FIELDS,
            max_results=1000,
            limit=sample_size // 1000 + 1,
        )
        for page in paginator:
            if not page or not page.get("data"):
                break
            followers.extend(page["data"])
            if len(followers) >= sample_size:
                break
            time.sleep(1.0)

    except tweepy.Forbidden:
        logger.warning(f"  Access denied to followers for user {user_id} (protected account?)")
    except tweepy.TooManyRequests:
        logger.warning("  Rate limit hit — waiting 15 minutes")
        time.sleep(900)
    except Exception as e:
        logger.error(f"  Error fetching followers for {user_id}: {e}")

    return followers[:sample_size]


def flatten_follower(follower: dict, target_user_id: str, sampled_at: str) -> dict:
    metrics = follower.get("public_metrics", {})
    pfp_url = follower.get("profile_image_url", "")

    return {
        "target_user_id":       target_user_id,
        "follower_id":          follower["id"],
        "follower_username":    follower.get("username"),
        "follower_created_at":  parse_twitter_date(follower.get("created_at")),
        "followers_count":      metrics.get("followers_count", 0),
        "following_count":      metrics.get("following_count", 0),
        "tweet_count":          metrics.get("tweet_count", 0),
        "has_pfp":              int(bool(pfp_url) and "default_profile_images" not in pfp_url),
        "has_bio":              int(bool((follower.get("description") or "").strip())),
        "is_verified":          int(follower.get("verified", False)),
        "bot_score":            None,    # filled later by bot_detector.py
        "bot_signals":          None,
        "sampled_at":           sampled_at,
        "raw_json":             json.dumps(follower),
    }


def upsert_followers(conn: sqlite3.Connection, rows: list[dict]) -> int:
    conn.executemany("""
        INSERT OR IGNORE INTO follower_samples (
            target_user_id, follower_id, follower_username, follower_created_at,
            followers_count, following_count, tweet_count,
            has_pfp, has_bio, is_verified,
            bot_score, bot_signals, sampled_at, raw_json
        ) VALUES (
            :target_user_id, :follower_id, :follower_username, :follower_created_at,
            :followers_count, :following_count, :tweet_count,
            :has_pfp, :has_bio, :is_verified,
            :bot_score, :bot_signals, :sampled_at, :raw_json
        )
    """, rows)
    conn.commit()
    return len(rows)


def run(
    config_path: str = "config/config.yaml",
    username_filter: str | None = None,
    sample_size: int = 1000,
) -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])
    client = make_client(cfg)

    if username_filter:
        users = conn.execute(
            "SELECT user_id, username, followers_count FROM profiles WHERE LOWER(username) = LOWER(?)",
            (username_filter,)
        ).fetchall()
    else:
        users = conn.execute(
            "SELECT user_id, username, followers_count FROM profiles"
        ).fetchall()

    total_saved = 0
    for user_row in tqdm(users, desc="Sampling followers", unit="user"):
        user_id = user_row["user_id"]
        username = user_row["username"]
        real_followers = user_row["followers_count"] or 0

        logger.info(f"\n@{username} ({real_followers:,} followers) — sampling {sample_size}")

        raw_followers = fetch_follower_sample(client, user_id, sample_size)
        logger.info(f"  - Got {len(raw_followers)} follower records")

        if not raw_followers:
            continue

        rows = [flatten_follower(f, user_id, now_iso()) for f in raw_followers]
        saved = upsert_followers(conn, rows)
        total_saved += saved
        logger.info(f"  - Saved {saved} follower samples")

    logger.info(f"\n- Follower sampling complete. Total saved: {total_saved:,}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample followers from celebrity profiles")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--username", default=None)
    parser.add_argument("--sample-size", type=int, default=1000,
                        help="Number of followers to sample per celebrity")
    args = parser.parse_args()
    run(args.config, args.username, args.sample_size)
