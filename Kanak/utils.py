"""
utils.py — Shared utilities: config loading, logging, rate-limit helpers,
           Tweepy client factory, and misc data helpers.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import colorlog
import tweepy
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential


# -- Config --------------------------------------------------------------------

def load_config(path: str = "config/config.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    # Allow environment variable overrides for secrets
    api = cfg.setdefault("api", {})
    for key in ("bearer_token", "api_key", "api_secret", "access_token", "access_token_secret"):
        env_key = f"X_{key.upper()}"
        if os.environ.get(env_key):
            api[key] = os.environ[env_key]

    return cfg


# -- Logging -------------------------------------------------------------------

def get_logger(name: str, log_dir: str = "logs", level: int = logging.INFO) -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)

    # Console — coloured
    ch = colorlog.StreamHandler()
    ch.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(name)s] %(levelname)s%(reset)s  %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG": "cyan", "INFO": "green",
            "WARNING": "yellow", "ERROR": "red", "CRITICAL": "bold_red"
        }
    ))
    logger.addHandler(ch)

    # File — plain
    fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

    return logger


# -- Tweepy Client Factory -----------------------------------------------------

def make_client(cfg: Dict[str, Any]) -> tweepy.Client:
    """Build a Tweepy v2 Client with user-auth (OAuth 1.0a) + bearer token."""
    api_cfg = cfg["api"]
    return tweepy.Client(
        bearer_token=api_cfg["bearer_token"],
        consumer_key=api_cfg["api_key"],
        consumer_secret=api_cfg["api_secret"],
        access_token=api_cfg["access_token"],
        access_token_secret=api_cfg["access_token_secret"],
        wait_on_rate_limit=True,   # Tweepy will sleep automatically
        return_type=dict,          # return raw dicts rather than Response objects
    )


def make_v1_api(cfg: Dict[str, Any]) -> tweepy.API:
    """Build a Tweepy v1.1 API object (needed for trends endpoint)."""
    api_cfg = cfg["api"]
    auth = tweepy.OAuth1UserHandler(
        api_cfg["api_key"], api_cfg["api_secret"],
        api_cfg["access_token"], api_cfg["access_token_secret"]
    )
    return tweepy.API(auth, wait_on_rate_limit=True)


# -- Rate-limit-aware request wrapper -----------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    reraise=True,
)
def safe_request(func, *args, delay: float = 1.0, **kwargs):
    """Call `func(*args, **kwargs)` with a post-call delay and auto-retry."""
    result = func(*args, **kwargs)
    time.sleep(delay)
    return result


# -- Date helpers --------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_twitter_date(s: Optional[str]) -> Optional[str]:
    """Normalise any Twitter date string to ISO-8601 UTC."""
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            dt = datetime.strptime(s, "%a %b %d %H:%M:%S +0000 %Y").replace(tzinfo=timezone.utc)
        except ValueError:
            return s
    return dt.isoformat()


# -- Tweet field sets (reuse everywhere) ---------------------------------------

TWEET_FIELDS = [
    "id", "text", "author_id", "created_at", "lang",
    "public_metrics", "non_public_metrics", "organic_metrics",
    "referenced_tweets", "source", "attachments", "entities",
    "conversation_id", "in_reply_to_user_id",
]

USER_FIELDS = [
    "id", "name", "username", "description", "location", "url",
    "profile_image_url", "verified", "created_at",
    "public_metrics", "entities", "withheld",
]

EXPANSIONS = [
    "author_id", "referenced_tweets.id", "attachments.media_keys",
    "in_reply_to_user_id",
]

MEDIA_FIELDS = ["media_key", "type", "url", "preview_image_url"]


# -- Data-shaping helpers ------------------------------------------------------

def extract_hashtags(entities: Optional[Dict]) -> str:
    if not entities:
        return ""
    tags = entities.get("hashtags", [])
    return ",".join(t.get("tag", "").lower() for t in tags)


def extract_mentions(entities: Optional[Dict]) -> str:
    if not entities:
        return ""
    mentions = entities.get("mentions", [])
    return ",".join(m.get("id", "") for m in mentions)


def extract_media_types(attachments: Optional[Dict], media_map: Dict) -> str:
    if not attachments:
        return ""
    keys = attachments.get("media_keys", [])
    types = [media_map.get(k, {}).get("type", "unknown") for k in keys]
    return ",".join(types)


def compute_engagement_rate(metrics: Dict, followers: int) -> float:
    if not followers or followers == 0:
        return 0.0
    total = (
        metrics.get("like_count", 0)
        + metrics.get("retweet_count", 0)
        + metrics.get("reply_count", 0)
        + metrics.get("quote_count", 0)
    )
    return round(total / followers, 6)


def flatten_tweet(tweet: Dict, author_followers: int, media_map: Dict, scraped_at: str) -> Dict:
    """Convert a raw API tweet dict into a flat row ready for the DB."""
    metrics = tweet.get("public_metrics", {})
    entities = tweet.get("entities", {})
    attachments = tweet.get("attachments", {})

    ref_tweets = tweet.get("referenced_tweets", [])
    ref_type_map = {r["type"]: r["id"] for r in ref_tweets} if ref_tweets else {}

    return {
        "tweet_id":             tweet["id"],
        "user_id":              tweet.get("author_id"),
        "text":                 tweet.get("text"),
        "lang":                 tweet.get("lang"),
        "created_at":           parse_twitter_date(tweet.get("created_at")),
        "like_count":           metrics.get("like_count", 0),
        "retweet_count":        metrics.get("retweet_count", 0),
        "reply_count":          metrics.get("reply_count", 0),
        "quote_count":          metrics.get("quote_count", 0),
        "bookmark_count":       metrics.get("bookmark_count", 0),
        "impression_count":     metrics.get("impression_count", 0),
        "is_retweet":           int("retweeted" in ref_type_map),
        "is_quote":             int("quoted" in ref_type_map),
        "is_reply":             int(bool(tweet.get("in_reply_to_user_id"))),
        "referenced_tweet_id":  ref_type_map.get("retweeted") or ref_type_map.get("replied_to"),
        "source":               tweet.get("source"),
        "has_media":            int(bool(attachments)),
        "media_types":          extract_media_types(attachments, media_map),
        "hashtags":             extract_hashtags(entities),
        "mentions":             extract_mentions(entities),
        "urls_count":           len(entities.get("urls", [])) if entities else 0,
        "engagement_rate":      compute_engagement_rate(metrics, author_followers),
        "scraped_at":           scraped_at,
        "raw_json":             json.dumps(tweet),
    }
