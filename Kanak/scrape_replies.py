"""
scrape_replies.py — Fetch replies/comments on each celebrity's posts.

Strategy: For each post in our DB, search for replies using
conversation_id. Captures author metadata needed for bot scoring.

Usage:
    python scrape_replies.py
    python scrape_replies.py --username KimKardashian --limit 100
"""

import argparse
import json
import sqlite3
from datetime import datetime, timezone

import tweepy
from tqdm import tqdm

from db import get_conn, init_db
from utils import (
    get_logger, load_config, make_client,
    now_iso, parse_twitter_date, safe_request
)

logger = get_logger("reply_scraper")

REPLY_TWEET_FIELDS = [
    "id", "text", "author_id", "created_at", "lang",
    "public_metrics", "in_reply_to_user_id", "conversation_id",
]

REPLY_USER_FIELDS = [
    "id", "name", "username", "description", "profile_image_url",
    "verified", "created_at", "public_metrics",
]


def search_replies(
    client: tweepy.Client,
    conversation_id: str,
    author_id: str,
    max_replies: int = 200,
) -> tuple[list[dict], dict]:
    """
    Search for replies to a conversation. Returns (tweet_list, users_by_id).
    """
    query = f"conversation_id:{conversation_id} is:reply -is:retweet"
    tweets = []
    users_by_id = {}

    try:
        paginator = tweepy.Paginator(
            client.search_recent_tweets,
            query=query,
            tweet_fields=REPLY_TWEET_FIELDS,
            user_fields=REPLY_USER_FIELDS,
            expansions=["author_id"],
            max_results=100,
            limit=max_replies // 100 + 1,
        )

        for page in paginator:
            if not page or not page.get("data"):
                continue

            includes = page.get("includes", {})
            for u in includes.get("users", []):
                users_by_id[u["id"]] = u

            tweets.extend(page["data"])
            if len(tweets) >= max_replies:
                break

    except tweepy.TooManyRequests:
        logger.warning("Rate-limit hit on reply search — will retry automatically")
    except Exception as e:
        logger.debug(f"Reply search error for conv {conversation_id}: {e}")

    return tweets, users_by_id


def flatten_reply(
    tweet: dict,
    parent_tweet_id: str,
    author: dict,
    scraped_at: str,
) -> dict:
    metrics = tweet.get("public_metrics", {})
    a_metrics = author.get("public_metrics", {}) if author else {}

    has_pfp = int(
        bool(author.get("profile_image_url"))
        and "default_profile_images" not in (author.get("profile_image_url") or "")
    )
    has_bio = int(bool((author.get("description") or "").strip()))

    return {
        "reply_id":           tweet["id"],
        "parent_tweet_id":    parent_tweet_id,
        "author_id":          tweet.get("author_id", ""),
        "author_username":    author.get("username") if author else None,
        "text":               tweet.get("text"),
        "lang":               tweet.get("lang"),
        "created_at":         parse_twitter_date(tweet.get("created_at")),
        "like_count":         metrics.get("like_count", 0),
        "reply_count":        metrics.get("reply_count", 0),
        "retweet_count":      metrics.get("retweet_count", 0),
        "author_followers":   a_metrics.get("followers_count"),
        "author_following":   a_metrics.get("following_count"),
        "author_tweet_count": a_metrics.get("tweet_count"),
        "author_created_at":  parse_twitter_date(author.get("created_at")) if author else None,
        "author_verified":    int(author.get("verified", False)) if author else 0,
        "author_has_pfp":     has_pfp,
        "author_has_bio":     has_bio,
        "bot_score":          None,   # computed later by bot_detector.py
        "bot_signals":        None,
        "scraped_at":         scraped_at,
        "raw_json":           json.dumps(tweet),
    }


def upsert_replies(conn: sqlite3.Connection, rows: list[dict]) -> int:
    conn.executemany("""
        INSERT INTO replies (
            reply_id, parent_tweet_id, author_id, author_username, text, lang,
            created_at, like_count, reply_count, retweet_count,
            author_followers, author_following, author_tweet_count,
            author_created_at, author_verified, author_has_pfp, author_has_bio,
            bot_score, bot_signals, scraped_at, raw_json
        ) VALUES (
            :reply_id, :parent_tweet_id, :author_id, :author_username, :text, :lang,
            :created_at, :like_count, :reply_count, :retweet_count,
            :author_followers, :author_following, :author_tweet_count,
            :author_created_at, :author_verified, :author_has_pfp, :author_has_bio,
            :bot_score, :bot_signals, :scraped_at, :raw_json
        )
        ON CONFLICT(reply_id) DO NOTHING
    """, rows)
    conn.commit()
    return len(rows)


def get_posts_for_user(conn: sqlite3.Connection, user_id: str, limit: int | None = None) -> list[sqlite3.Row]:
    q = """
        SELECT tweet_id
        FROM posts
        WHERE user_id = ?
          AND is_retweet = 0
        ORDER BY like_count DESC
    """
    if limit:
        q += f" LIMIT {limit}"
    return conn.execute(q, (user_id,)).fetchall()


def run(
    config_path: str = "config/config.yaml",
    username_filter: str | None = None,
    posts_per_user: int = 50,
    replies_per_post: int = 200,
) -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])
    client = make_client(cfg)

    # Get all profiled users (or just one)
    if username_filter:
        users = conn.execute(
            "SELECT user_id, username FROM profiles WHERE LOWER(username) = LOWER(?)",
            (username_filter,)
        ).fetchall()
    else:
        users = conn.execute("SELECT user_id, username FROM profiles").fetchall()

    total_replies = 0

    for user_row in tqdm(users, desc="Users", unit="user"):
        user_id = user_row["user_id"]
        username = user_row["username"]
        logger.info(f"\n@{username} — fetching top {posts_per_user} posts' replies")

        posts = get_posts_for_user(conn, user_id, limit=posts_per_user)
        logger.info(f"  Found {len(posts)} posts in DB")

        for post_row in tqdm(posts, desc=f"  Posts @{username}", leave=False):
            tweet_id = post_row["tweet_id"]
            try:
                reply_tweets, users_by_id = search_replies(
                    client, tweet_id, user_id, max_replies=replies_per_post
                )
                if not reply_tweets:
                    continue

                rows = []
                for t in reply_tweets:
                    author = users_by_id.get(t.get("author_id", ""))
                    rows.append(flatten_reply(t, tweet_id, author, now_iso()))

                saved = upsert_replies(conn, rows)
                total_replies += saved
                logger.debug(f"    tweet {tweet_id}: saved {saved} replies")

            except Exception as e:
                logger.error(f"    Error on tweet {tweet_id}: {e}")

    logger.info(f"\n- Reply scrape complete. Total replies saved: {total_replies:,}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape replies to celebrity posts")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--username", default=None)
    parser.add_argument("--posts-per-user", type=int, default=50,
                        help="How many posts (sorted by likes) to fetch replies for")
    parser.add_argument("--replies-per-post", type=int, default=200,
                        help="Max replies to collect per post")
    args = parser.parse_args()
    run(args.config, args.username, args.posts_per_user, args.replies_per_post)
