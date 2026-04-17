"""
scrape_trending.py — Capture trending topics data.

Two modes:
  1. LIVE: Poll Twitter's trending topics API every hour and store snapshots.
  2. HISTORICAL (search-based): For each known trend, pull tweets mentioning
     it in a date range to reconstruct volume + engagement patterns.

Usage:
    python scrape_trending.py --mode live       # poll now + schedule hourly
    python scrape_trending.py --mode snapshot   # one-off capture right now
    python scrape_trending.py --mode historical --trend "#OscarsSoWhite"
"""

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone

import schedule
import tweepy
from tqdm import tqdm

from db import get_conn, init_db
from utils import get_logger, load_config, make_client, make_v1_api, now_iso

logger = get_logger("trending_scraper")


# -- Live trending snapshot ----------------------------------------------------

def fetch_trending_snapshot(v1_api: tweepy.API, woeids: list[int]) -> list[dict]:
    """
    Fetch trending topics for each WOEID using Twitter v1.1 trends/place.
    Returns flat list of trend records.
    """
    records = []
    captured_at = now_iso()

    for woeid in woeids:
        try:
            result = v1_api.get_place_trends(woeid)
            if not result:
                continue

            location_name = result[0].get("locations", [{}])[0].get("name", str(woeid))
            trends = result[0].get("trends", [])

            for rank, trend in enumerate(trends, start=1):
                records.append({
                    "woeid":          woeid,
                    "location_name":  location_name,
                    "trend_name":     trend.get("name", ""),
                    "query":          trend.get("query", ""),
                    "tweet_volume":   trend.get("tweet_volume"),  # can be None
                    "rank":           rank,
                    "captured_at":    captured_at,
                })

        except tweepy.TooManyRequests:
            logger.warning(f"Rate limit on trends for WOEID {woeid}")
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error fetching trends for WOEID {woeid}: {e}")

    return records


def save_trending(conn: sqlite3.Connection, records: list[dict]) -> int:
    conn.executemany("""
        INSERT INTO trending_topics (
            woeid, location_name, trend_name, query,
            tweet_volume, rank, captured_at
        ) VALUES (
            :woeid, :location_name, :trend_name, :query,
            :tweet_volume, :rank, :captured_at
        )
    """, records)
    conn.commit()
    return len(records)


# -- Historical trend reconstruction via search -----------------------------

SEARCH_TWEET_FIELDS = [
    "id", "text", "author_id", "created_at", "lang",
    "public_metrics", "referenced_tweets", "entities",
]

SEARCH_USER_FIELDS = [
    "id", "username", "verified", "created_at", "public_metrics",
    "description", "profile_image_url",
]


def search_trend_tweets(
    client: tweepy.Client,
    query: str,
    start_time: str,
    end_time: str,
    max_results: int = 500,
) -> list[dict]:
    """
    Use full-archive search to find tweets matching a trend query.
    Requires Academic Research / Pro tier.
    """
    full_query = f"({query}) -is:retweet lang:en"
    tweets = []
    users_by_id = {}

    try:
        paginator = tweepy.Paginator(
            client.search_all_tweets,   # full-archive
            query=full_query,
            tweet_fields=SEARCH_TWEET_FIELDS,
            user_fields=SEARCH_USER_FIELDS,
            expansions=["author_id"],
            start_time=start_time,
            end_time=end_time,
            max_results=100,
            limit=max_results // 100 + 1,
        )

        for page in paginator:
            if not page or not page.get("data"):
                continue

            includes = page.get("includes", {})
            for u in includes.get("users", []):
                users_by_id[u["id"]] = u

            for tweet in page["data"]:
                author = users_by_id.get(tweet.get("author_id", ""), {})
                metrics = tweet.get("public_metrics", {})
                a_metrics = author.get("public_metrics", {})
                tweets.append({
                    "tweet_id":         tweet["id"],
                    "text":             tweet.get("text"),
                    "author_id":        tweet.get("author_id"),
                    "author_username":  author.get("username"),
                    "author_followers": a_metrics.get("followers_count", 0),
                    "author_verified":  int(author.get("verified", False)),
                    "author_has_pfp":   int(
                        bool(author.get("profile_image_url"))
                        and "default_profile_images" not in (author.get("profile_image_url") or "")
                    ),
                    "author_has_bio":   int(bool((author.get("description") or "").strip())),
                    "created_at":       tweet.get("created_at"),
                    "like_count":       metrics.get("like_count", 0),
                    "retweet_count":    metrics.get("retweet_count", 0),
                    "reply_count":      metrics.get("reply_count", 0),
                    "quote_count":      metrics.get("quote_count", 0),
                    "lang":             tweet.get("lang"),
                    "query":            query,
                })

            if len(tweets) >= max_results:
                break

    except tweepy.BadRequest as e:
        logger.error(f"Bad search query '{full_query}': {e}")
    except tweepy.Forbidden:
        logger.error("Full-archive search requires Academic/Pro API tier")
    except Exception as e:
        logger.error(f"Search error for '{query}': {e}")

    return tweets


def save_trend_tweet_sample(conn: sqlite3.Connection, tweets: list[dict], trend_name: str) -> None:
    """Save tweet samples for a historical trend to a CSV for analysis."""
    import csv
    import os

    out_dir = "data/trending"
    os.makedirs(out_dir, exist_ok=True)
    safe_name = trend_name.replace("#", "").replace(" ", "_").replace("/", "_")[:50]
    path = os.path.join(out_dir, f"{safe_name}_tweets.csv")

    if not tweets:
        return

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(tweets[0].keys()))
        writer.writeheader()
        writer.writerows(tweets)

    logger.info(f"  Saved {len(tweets)} trend tweets - {path}")


# -- Scheduler -----------------------------------------------------------------

def one_snapshot(cfg: dict, conn: sqlite3.Connection, v1_api: tweepy.API) -> None:
    woeids = [
        cfg["trending"]["woeid_global"],
        cfg["trending"]["woeid_us"],
        cfg["trending"]["woeid_uk"],
    ]
    records = fetch_trending_snapshot(v1_api, woeids)
    saved = save_trending(conn, records)
    logger.info(f"Trending snapshot: {saved} trends saved (woeids={woeids})")


def run(
    config_path: str = "config/config.yaml",
    mode: str = "snapshot",
    trend_query: str | None = None,
    start_date: str = "2020-01-01",
    end_date: str | None = None,
) -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])
    client = make_client(cfg)
    v1_api = make_v1_api(cfg)

    if mode == "snapshot":
        one_snapshot(cfg, conn, v1_api)

    elif mode == "live":
        logger.info("Starting live trending collector — running every hour")
        one_snapshot(cfg, conn, v1_api)   # run immediately
        schedule.every(
            cfg["trending"].get("collection_frequency_hours", 1)
        ).hours.do(one_snapshot, cfg, conn, v1_api)

        while True:
            schedule.run_pending()
            time.sleep(30)

    elif mode == "historical":
        if not trend_query:
            logger.error("--trend is required for historical mode")
            return

        end = (end_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")) + "T23:59:59Z"
        start = start_date + "T00:00:00Z"

        logger.info(f"Historical search for '{trend_query}' from {start} to {end}")
        tweets = search_trend_tweets(client, trend_query, start, end, max_results=1000)
        save_trend_tweet_sample(conn, tweets, trend_query)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect X trending topic data")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--mode", choices=["snapshot", "live", "historical"], default="snapshot")
    parser.add_argument("--trend", default=None, help="Trend query for historical mode")
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args()
    run(args.config, args.mode, args.trend, args.start_date, args.end_date)
