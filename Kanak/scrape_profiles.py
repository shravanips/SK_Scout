"""
scrape_profiles.py — Fetch and store profile data for all target celebrities.

Usage:
    python scrape_profiles.py
    python scrape_profiles.py --config config/config.yaml
"""

import argparse
import json
import sqlite3
from datetime import datetime, timezone

import tweepy

from db import get_conn, init_db
from utils import (
    USER_FIELDS, get_logger, load_config, make_client,
    now_iso, parse_twitter_date, safe_request
)

logger = get_logger("profile_scraper")

# -- Category map (username - category) ---------------------------------------
CATEGORY_MAP = {
    "therock":          "actor",
    "priyankachopra":   "actor",
    "justinbieber":     "singer",
    "selenagomez":      "singer_actress",
    "taylorswift13":    "singer",
    "cristiano":        "athlete",
    "rihanna":          "singer",
    "ladygaga":         "singer",
    "britneyspears":    "singer",
    "mileycyrus":       "singer_actress",
    "arianagrande":     "singer",
    "beyoncest":        "singer",
    "drake":            "singer",
    "elonmusk":         "tech_celebrity",
    "kimkardashian":    "reality_tv",
    "kyliejenner":      "reality_tv",
    "kendalljenner":    "reality_tv",
    "khloekardashian":  "reality_tv",
    "kourtneykardash":  "reality_tv",
    "nickiminaj":       "singer",
    "cardi b":          "singer",
    "justintimberlake": "singer",
    "adele":            "singer",
    "demilovato":       "singer_actress",
    "pink":             "singer",
}


def fetch_users_by_username(client: tweepy.Client, usernames: list[str]) -> list[dict]:
    """Fetch up to 100 users in a single batch request."""
    results = []
    # API allows max 100 per call
    for i in range(0, len(usernames), 100):
        batch = usernames[i:i + 100]
        try:
            response = safe_request(
                client.get_users,
                usernames=batch,
                user_fields=USER_FIELDS,
            )
            data = response.get("data", []) if response else []
            results.extend(data)
            logger.info(f"Fetched {len(data)} profiles (batch {i // 100 + 1})")
        except Exception as e:
            logger.error(f"Error fetching batch {batch}: {e}")
    return results


def upsert_profile(conn: sqlite3.Connection, user: dict, category: str, scraped_at: str) -> None:
    metrics = user.get("public_metrics", {})
    withheld = user.get("withheld", {})

    row = {
        "user_id":              user["id"],
        "username":             user.get("username"),
        "display_name":         user.get("name"),
        "profession_category":  category,
        "description":          user.get("description"),
        "location":             user.get("location"),
        "url":                  user.get("url"),
        "profile_image_url":    user.get("profile_image_url"),
        "is_verified":          int(user.get("verified", False)),
        "is_blue_verified":     int(user.get("is_blue_verified", False)),
        "followers_count":      metrics.get("followers_count", 0),
        "following_count":      metrics.get("following_count", 0),
        "tweet_count":          metrics.get("tweet_count", 0),
        "listed_count":         metrics.get("listed_count", 0),
        "account_created_at":   parse_twitter_date(user.get("created_at")),
        "withheld_countries":   json.dumps(withheld.get("country_codes", [])),
        "scraped_at":           scraped_at,
        "raw_json":             json.dumps(user),
    }

    conn.execute("""
        INSERT INTO profiles (
            user_id, username, display_name, profession_category,
            description, location, url, profile_image_url,
            is_verified, is_blue_verified, followers_count, following_count,
            tweet_count, listed_count, account_created_at, withheld_countries,
            scraped_at, raw_json
        ) VALUES (
            :user_id, :username, :display_name, :profession_category,
            :description, :location, :url, :profile_image_url,
            :is_verified, :is_blue_verified, :followers_count, :following_count,
            :tweet_count, :listed_count, :account_created_at, :withheld_countries,
            :scraped_at, :raw_json
        )
        ON CONFLICT(user_id) DO UPDATE SET
            display_name        = excluded.display_name,
            description         = excluded.description,
            location            = excluded.location,
            followers_count     = excluded.followers_count,
            following_count     = excluded.following_count,
            tweet_count         = excluded.tweet_count,
            listed_count        = excluded.listed_count,
            is_verified         = excluded.is_verified,
            is_blue_verified    = excluded.is_blue_verified,
            scraped_at          = excluded.scraped_at,
            raw_json            = excluded.raw_json
    """, row)

    # Snapshot
    conn.execute("""
        INSERT INTO profile_snapshots (user_id, followers_count, following_count, tweet_count, snapshot_at)
        VALUES (:user_id, :followers_count, :following_count, :tweet_count, :scraped_at)
    """, row)

    conn.commit()


def run(config_path: str = "config/config.yaml") -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])
    client = make_client(cfg)

    # Collect all usernames from config
    targets = cfg.get("targets", {})
    all_targets = []
    for category_list in targets.values():
        all_targets.extend(category_list)

    usernames = [t["username"] for t in all_targets]
    logger.info(f"Fetching profiles for {len(usernames)} celebrities")

    scraped_at = now_iso()
    users = fetch_users_by_username(client, usernames)

    saved = 0
    for user in users:
        uname_lower = user.get("username", "").lower()
        category = CATEGORY_MAP.get(uname_lower, "celebrity")
        upsert_profile(conn, user, category, scraped_at)
        logger.info(f"  - {user.get('username')} — {metrics_summary(user)}")
        saved += 1

    logger.info(f"Profile scrape complete. Saved {saved} profiles.")
    conn.close()


def metrics_summary(user: dict) -> str:
    m = user.get("public_metrics", {})
    return (
        f"followers={m.get('followers_count', 0):,}  "
        f"following={m.get('following_count', 0):,}  "
        f"tweets={m.get('tweet_count', 0):,}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape celebrity profiles from X")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    run(args.config)
