"""
bot_detector.py — Multi-signal bot detection for followers, commenters,
                  and engagement patterns on celebrity profiles.

Produces:
  - Per-follower bot scores in follower_samples
  - Per-reply bot scores in replies
  - Per-profile bot_analysis summary rows

Signals used
-------------
Account-level (followers & commenters):
  1. Account age         — very new accounts (<90 days) are suspicious
  2. Follower/following ratio — bots often follow many but have few followers
  3. No profile picture  — strong bot signal
  4. No bio              — moderate bot signal
  5. Tweet count         — bots post very little or hyper-actively
  6. Followers count     — bots often have near-zero followers

Engagement-level (per post):
  7. Engagement spike    — sudden like/RT explosion in <1 hr is suspicious
  8. Engagement rate     — implausibly high (>50%) or zombie low (<0.001%)
  9. Comment similarity  — clusters of near-identical comments = coordinated bots
  10. Comment timing     — bot comments arrive in tight bursts

Usage:
    python bot_detector.py                          # score everything
    python bot_detector.py --username KimKardashian  # one profile
    python bot_detector.py --mode followers          # only followers
    python bot_detector.py --mode comments           # only comments
    python bot_detector.py --mode engagement         # only post engagement
"""

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm

from db import get_conn, init_db
from utils import get_logger, load_config, now_iso

logger = get_logger("bot_detector")


# -- Account-level bot scorer --------------------------------------------------

def score_account(
    created_at_str: Optional[str],
    followers: Optional[int],
    following: Optional[int],
    tweet_count: Optional[int],
    has_pfp: int,
    has_bio: int,
    is_verified: int,
    cfg: dict,
) -> tuple[float, list[str]]:
    """
    Return (bot_score 0–1, list_of_triggered_signals).
    Higher score = more likely a bot.
    """
    signals = []
    score = 0.0
    bot_cfg = cfg["bot_detection"]

    # Verified accounts get a strong authenticity bonus
    if is_verified:
        score -= 0.3

    # No profile picture
    if not has_pfp:
        score += bot_cfg.get("default_profile_image_penalty", 0.3)
        signals.append("no_profile_picture")

    # No bio
    if not has_bio:
        score += bot_cfg.get("no_bio_penalty", 0.2)
        signals.append("no_bio")

    # Account age
    if created_at_str:
        try:
            created = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - created).days
            if age_days < bot_cfg.get("account_age_days_min", 90):
                factor = max(0.4, 1.0 - age_days / 90)
                score += 0.3 * factor
                signals.append(f"new_account_{age_days}d")
        except Exception:
            pass

    # Follower/following ratio — bots follow many, have few followers
    if following and following > 0 and followers is not None:
        ratio = followers / following
        max_ratio = bot_cfg.get("follower_following_ratio_max", 0.01)
        if ratio < max_ratio:
            score += 0.25
            signals.append(f"low_ff_ratio_{ratio:.4f}")
        elif ratio < 0.1:
            score += 0.1
            signals.append(f"moderate_low_ff_ratio_{ratio:.3f}")

    # Tweet count extremes
    if tweet_count is not None:
        if tweet_count == 0:
            score += 0.2
            signals.append("zero_tweets")
        elif tweet_count < 5:
            score += 0.1
            signals.append("very_few_tweets")
        elif tweet_count > 100_000:
            score += 0.15
            signals.append("hyper_active_tweeter")

    # Near-zero followers (but not zero — zero following is just new)
    if followers is not None and followers < 5 and following and following > 50:
        score += 0.2
        signals.append("near_zero_followers_high_following")

    return round(min(max(score, 0.0), 1.0), 4), signals


# -- Comment similarity detector -----------------------------------------------

def find_duplicate_comment_clusters(texts: list[str], threshold: float = 0.85) -> int:
    """
    Detect clusters of very similar comments using TF-IDF + cosine similarity.
    Returns number of suspicious comment clusters found.
    """
    if len(texts) < 5:
        return 0

    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        vectorizer = TfidfVectorizer(max_features=1000, stop_words="english")
        tfidf = vectorizer.fit_transform(texts)
        sim_matrix = cosine_similarity(tfidf)

        # Count pairs above threshold (excluding self-similarity diagonal)
        np.fill_diagonal(sim_matrix, 0)
        suspicious_pairs = int((sim_matrix > threshold).sum() // 2)

        # Rough cluster estimate: suspicious_pairs / avg_cluster_size
        clusters = max(0, suspicious_pairs // max(1, len(texts) // 10))
        return clusters

    except Exception as e:
        logger.debug(f"Comment similarity error: {e}")
        return 0


def detect_comment_timing_bursts(timestamps: list[str], window_minutes: int = 5) -> int:
    """
    Count suspicious timing bursts: windows where >10 comments arrive
    within `window_minutes` minutes.
    """
    if len(timestamps) < 10:
        return 0

    parsed = []
    for ts in timestamps:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            parsed.append(dt)
        except Exception:
            pass

    if not parsed:
        return 0

    parsed.sort()
    window = timedelta(minutes=window_minutes)
    burst_count = 0

    for i, t in enumerate(parsed):
        end = t + window
        # Count comments in this window
        count = sum(1 for t2 in parsed[i:] if t2 <= end)
        if count >= 10:
            burst_count += 1

    return burst_count


# -- Main scoring functions ----------------------------------------------------

def score_followers(conn: sqlite3.Connection, user_id: str, cfg: dict) -> tuple[int, float]:
    """Score all followers for a given user. Returns (count_scored, avg_bot_score)."""
    rows = conn.execute(
        """
        SELECT id, follower_created_at, followers_count, following_count,
               tweet_count, has_pfp, has_bio, is_verified
        FROM follower_samples
        WHERE target_user_id = ?
        """,
        (user_id,)
    ).fetchall()

    if not rows:
        return 0, 0.0

    scores = []
    for row in rows:
        score, signals = score_account(
            created_at_str=row["follower_created_at"],
            followers=row["followers_count"],
            following=row["following_count"],
            tweet_count=row["tweet_count"],
            has_pfp=row["has_pfp"] or 0,
            has_bio=row["has_bio"] or 0,
            is_verified=row["is_verified"] or 0,
            cfg=cfg,
        )
        scores.append(score)
        conn.execute(
            "UPDATE follower_samples SET bot_score = ?, bot_signals = ? WHERE id = ?",
            (score, json.dumps(signals), row["id"])
        )

    conn.commit()
    avg = round(float(np.mean(scores)), 4) if scores else 0.0
    return len(scores), avg


def score_comments(conn: sqlite3.Connection, user_id: str, cfg: dict) -> tuple[int, float, int]:
    """Score reply authors for a given user's posts. Returns (count, avg_score, clusters)."""
    rows = conn.execute(
        """
        SELECT r.reply_id, r.text, r.created_at,
               r.author_created_at, r.author_followers, r.author_following,
               r.author_tweet_count, r.author_has_pfp, r.author_has_bio, r.author_verified
        FROM replies r
        JOIN posts p ON r.parent_tweet_id = p.tweet_id
        WHERE p.user_id = ?
        """,
        (user_id,)
    ).fetchall()

    if not rows:
        return 0, 0.0, 0

    scores = []
    texts = []
    timestamps = []

    for row in rows:
        score, signals = score_account(
            created_at_str=row["author_created_at"],
            followers=row["author_followers"],
            following=row["author_following"],
            tweet_count=row["author_tweet_count"],
            has_pfp=row["author_has_pfp"] or 0,
            has_bio=row["author_has_bio"] or 0,
            is_verified=row["author_verified"] or 0,
            cfg=cfg,
        )
        scores.append(score)
        if row["text"]:
            texts.append(row["text"])
        if row["created_at"]:
            timestamps.append(row["created_at"])

        conn.execute(
            "UPDATE replies SET bot_score = ?, bot_signals = ? WHERE reply_id = ?",
            (score, json.dumps(signals), row["reply_id"])
        )

    conn.commit()

    # Aggregate signals
    clusters = find_duplicate_comment_clusters(
        texts, threshold=cfg["bot_detection"]["suspicious_comment_similarity_threshold"]
    )
    bursts = detect_comment_timing_bursts(timestamps)

    avg = round(float(np.mean(scores)), 4) if scores else 0.0
    return len(scores), avg, clusters


def score_engagement(conn: sqlite3.Connection, user_id: str, cfg: dict) -> dict:
    """
    Analyse engagement patterns on all posts for a user.
    Returns dict of engagement bot signals.
    """
    rows = conn.execute(
        """
        SELECT tweet_id, like_count, retweet_count, reply_count, quote_count,
               engagement_rate, created_at
        FROM posts
        WHERE user_id = ?
          AND is_retweet = 0
        ORDER BY created_at ASC
        """,
        (user_id,)
    ).fetchall()

    if not rows:
        return {}

    df = pd.DataFrame([dict(r) for r in rows])
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["total_engagement"] = (
        df["like_count"] + df["retweet_count"] + df["reply_count"] + df["quote_count"]
    )

    bot_cfg = cfg["bot_detection"]
    min_er = bot_cfg["min_engagement_rate"]
    max_er = bot_cfg["max_engagement_rate"]

    # Posts with suspiciously high engagement rate
    suspicious_high = int((df["engagement_rate"] > max_er).sum())
    zombie_low = int((df["engagement_rate"] < min_er).sum())

    # Engagement spikes: posts where engagement is >5σ above mean
    mean_eng = df["total_engagement"].mean()
    std_eng = df["total_engagement"].std()
    spike_threshold = mean_eng + 5 * std_eng if std_eng > 0 else mean_eng * 10
    spikes = int((df["total_engagement"] > spike_threshold).sum())

    # Engagement over time — look for unnatural flatness or sudden jumps
    er_std = float(df["engagement_rate"].std())
    er_mean = float(df["engagement_rate"].mean())
    cv = er_std / er_mean if er_mean > 0 else 0

    signals = []
    eng_score = 0.0

    if suspicious_high > 0:
        signals.append(f"{suspicious_high}_posts_unrealistic_er")
        eng_score += min(0.3, suspicious_high / len(df) * 2)

    if spikes > 0:
        signals.append(f"{spikes}_engagement_spikes")
        eng_score += min(0.2, spikes / len(df))

    if cv < 0.1 and len(df) > 20:
        signals.append("suspiciously_uniform_engagement")
        eng_score += 0.15

    return {
        "total_posts": len(df),
        "suspicious_high_er_posts": suspicious_high,
        "zombie_engagement_posts": zombie_low,
        "engagement_spikes": spikes,
        "engagement_coefficient_of_variation": round(cv, 4),
        "engagement_bot_score": round(min(eng_score, 1.0), 4),
        "signals": signals,
    }


# -- Profile-level summary -----------------------------------------------------

def analyse_profile(conn: sqlite3.Connection, user_id: str, cfg: dict) -> None:
    logger.info(f"  Analysing {user_id}...")

    n_followers, follower_bot_score = score_followers(conn, user_id, cfg)
    n_comments, comment_bot_score, dup_clusters = score_comments(conn, user_id, cfg)
    engagement = score_engagement(conn, user_id, cfg)

    # Estimate bot follower %
    bot_follower_pct = round(follower_bot_score * 100, 2)

    # Compute overall score (weighted average)
    weights = {"followers": 0.4, "comments": 0.3, "engagement": 0.3}
    overall = (
        follower_bot_score * weights["followers"]
        + comment_bot_score * weights["comments"]
        + engagement.get("engagement_bot_score", 0) * weights["engagement"]
    )
    overall = round(overall, 4)

    # Verdict
    if overall < 0.15:
        verdict = "clean"
    elif overall < 0.30:
        verdict = "moderate"
    elif overall < 0.55:
        verdict = "high_bot"
    else:
        verdict = "severe"

    key_signals = (
        [f"~{bot_follower_pct}%_bot_followers"] +
        ([f"{dup_clusters}_duplicate_comment_clusters"] if dup_clusters else []) +
        engagement.get("signals", [])
    )

    conn.execute("""
        INSERT INTO bot_analysis (
            user_id, analysis_date, total_posts_analyzed,
            total_replies_analyzed, total_followers_sampled,
            estimated_bot_followers_pct, follower_bot_score,
            avg_post_bot_engagement_pct, engagement_bot_score,
            comment_bot_pct, duplicate_comment_clusters,
            overall_bot_score, overall_verdict, key_signals
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id, now_iso(),
        engagement.get("total_posts", 0), n_comments, n_followers,
        bot_follower_pct, follower_bot_score,
        round(comment_bot_score * 100, 2), engagement.get("engagement_bot_score", 0),
        round(comment_bot_score * 100, 2), dup_clusters,
        overall, verdict, json.dumps(key_signals),
    ))
    conn.commit()
    logger.info(f"  - verdict={verdict}  overall_score={overall}  signals={key_signals}")


# -- Entry point ---------------------------------------------------------------

def run(
    config_path: str = "config/config.yaml",
    username_filter: str | None = None,
    mode: str = "all",
) -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])

    if username_filter:
        users = conn.execute(
            "SELECT user_id, username FROM profiles WHERE LOWER(username) = LOWER(?)",
            (username_filter,)
        ).fetchall()
    else:
        users = conn.execute("SELECT user_id, username FROM profiles").fetchall()

    logger.info(f"Running bot detection on {len(users)} profiles (mode={mode})")

    for user_row in tqdm(users, desc="Bot detection", unit="profile"):
        user_id = user_row["user_id"]
        username = user_row["username"]
        logger.info(f"\n@{username}")

        try:
            if mode in ("all", "followers"):
                n, avg = score_followers(conn, user_id, cfg)
                logger.info(f"  Followers: {n} scored, avg_bot={avg}")

            if mode in ("all", "comments"):
                n, avg, clusters = score_comments(conn, user_id, cfg)
                logger.info(f"  Comments: {n} scored, avg_bot={avg}, clusters={clusters}")

            if mode in ("all", "engagement"):
                eng = score_engagement(conn, user_id, cfg)
                logger.info(f"  Engagement: {eng}")

            if mode == "all":
                analyse_profile(conn, user_id, cfg)

        except Exception as e:
            logger.error(f"Error analysing @{username}: {e}")

    logger.info("\n- Bot detection complete.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run bot detection on scraped X data")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--username", default=None)
    parser.add_argument("--mode", choices=["all", "followers", "comments", "engagement"], default="all")
    args = parser.parse_args()
    run(args.config, args.username, args.mode)
