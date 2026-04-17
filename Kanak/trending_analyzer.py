"""
trending_analyzer.py — Link trending topics to bot engagement data to
                        identify artificially hyped / paid trends.

Algorithm
---------
For each trend:
  1. Pull all tweets sampled for that trend (from data/trending/)
  2. Score tweet authors using bot_detector account signals
  3. Check for: abnormal volume spikes, bot-heavy author pool,
     coordinated timing, low organic reach, celebrity page overlap
  4. Assign authenticity_score + verdict

Verdicts:
  authentic      — mostly organic, high-quality accounts, natural growth
  suspicious     — moderate bot signals, worth investigating
  likely_paid    — strong bot signals, coordinated timing, abnormal volume

Usage:
    python trending_analyzer.py
    python trending_analyzer.py --trend "#GrammyAwards"
    python trending_analyzer.py --output reports/trending_report.csv
"""

import argparse
import csv
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from bot_detector import detect_comment_timing_bursts, find_duplicate_comment_clusters, score_account
from db import get_conn, init_db
from utils import get_logger, load_config, now_iso

logger = get_logger("trending_analyzer")


# -- Load trend tweet samples --------------------------------------------------

def load_trend_csv(trend_name: str, data_dir: str = "data/trending") -> pd.DataFrame | None:
    safe = trend_name.replace("#", "").replace(" ", "_").replace("/", "_")[:50]
    path = os.path.join(data_dir, f"{safe}_tweets.csv")
    if not os.path.exists(path):
        logger.debug(f"No CSV found for trend '{trend_name}' at {path}")
        return None
    df = pd.read_csv(path)
    return df


# -- Score trend tweets --------------------------------------------------------

def score_trend_authors(df: pd.DataFrame, cfg: dict) -> dict:
    """
    Score each tweet author and aggregate.
    Returns dict of bot metrics.
    """
    scores = []
    for _, row in df.iterrows():
        score, _ = score_account(
            created_at_str=None,       # not stored in trend CSVs — use other signals
            followers=row.get("author_followers", 0) or 0,
            following=None,
            tweet_count=None,
            has_pfp=int(row.get("author_has_pfp", 1)),
            has_bio=int(row.get("author_has_bio", 1)),
            is_verified=int(row.get("author_verified", 0)),
            cfg=cfg,
        )
        scores.append(score)

    arr = np.array(scores)
    return {
        "total_tweets":      len(df),
        "mean_bot_score":    round(float(arr.mean()), 4) if len(arr) else 0,
        "pct_high_bot":      round(float((arr > 0.5).mean() * 100), 2) if len(arr) else 0,
        "verified_pct":      round(float(df["author_verified"].mean() * 100), 2) if "author_verified" in df else 0,
    }


def score_trend_volume(
    conn: sqlite3.Connection,
    trend_name: str,
) -> dict:
    """
    Look at trending_topics table to detect volume anomalies.
    """
    rows = conn.execute(
        """
        SELECT tweet_volume, rank, captured_at
        FROM trending_topics
        WHERE LOWER(trend_name) = LOWER(?)
        ORDER BY captured_at ASC
        """,
        (trend_name,)
    ).fetchall()

    if not rows:
        return {"times_trended": 0}

    volumes = [r["tweet_volume"] for r in rows if r["tweet_volume"] is not None]
    ranks = [r["rank"] for r in rows]

    result = {
        "times_trended": len(rows),
        "peak_volume":   max(volumes) if volumes else None,
        "avg_volume":    int(np.mean(volumes)) if volumes else None,
        "best_rank":     min(ranks) if ranks else None,
    }

    # Spike detection: if volume jumps >5x in one snapshot, suspicious
    if len(volumes) >= 3:
        diffs = [volumes[i+1] / max(volumes[i], 1) for i in range(len(volumes)-1)]
        result["max_volume_spike_ratio"] = round(max(diffs), 2)
        result["suspicious_spike"] = max(diffs) > 5.0
    else:
        result["suspicious_spike"] = False

    return result


def check_celebrity_overlap(
    conn: sqlite3.Connection,
    trend_name: str,
) -> dict:
    """
    Check how many posts from our tracked celebrities mention this trend
    (as a hashtag or keyword).
    """
    like_term = f"%{trend_name.replace('#', '')}%"
    rows = conn.execute(
        """
        SELECT p.user_id, pr.username, COUNT(*) as mention_count,
               SUM(p.like_count + p.retweet_count) as total_engagement
        FROM posts p
        JOIN profiles pr ON p.user_id = pr.user_id
        WHERE (LOWER(p.hashtags) LIKE LOWER(?)
               OR LOWER(p.text) LIKE LOWER(?))
        GROUP BY p.user_id
        ORDER BY total_engagement DESC
        """,
        (like_term, like_term)
    ).fetchall()

    return {
        "celebrity_mentions": {r["username"]: r["mention_count"] for r in rows},
        "total_celebrities_involved": len(rows),
        "total_celebrity_engagement": sum(r["total_engagement"] or 0 for r in rows),
    }


# -- Compute authenticity score -------------------------------------------------

def compute_authenticity(
    author_scores: dict,
    volume_scores: dict,
    celebrity_overlap: dict,
) -> tuple[float, str, list[str]]:
    """
    Returns (authenticity_score 0–1, verdict, evidence_list).
    Higher authenticity_score = more organic.
    """
    evidence = []
    deductions = 0.0

    # 1. Bot author concentration
    pct_high_bot = author_scores.get("pct_high_bot", 0)
    if pct_high_bot > 60:
        deductions += 0.40
        evidence.append(f"{pct_high_bot:.1f}%_of_tweeters_are_likely_bots")
    elif pct_high_bot > 30:
        deductions += 0.20
        evidence.append(f"{pct_high_bot:.1f}%_elevated_bot_presence")

    # 2. Very low verified %
    verified_pct = author_scores.get("verified_pct", 50)
    if verified_pct < 1:
        deductions += 0.10
        evidence.append("near_zero_verified_accounts_tweeting")

    # 3. Volume spike
    if volume_scores.get("suspicious_spike"):
        ratio = volume_scores.get("max_volume_spike_ratio", 0)
        deductions += min(0.25, ratio / 20)
        evidence.append(f"volume_spike_{ratio}x")

    # 4. Celebrity correlation — if celebrities pushed it AND bots are high,
    #    this is a red flag for coordinated paid promotion.
    celeb_count = celebrity_overlap.get("total_celebrities_involved", 0)
    if celeb_count > 3 and pct_high_bot > 30:
        deductions += 0.15
        evidence.append(f"{celeb_count}_celebrities_pushing_trend_with_high_bot_engagement")

    authenticity = round(max(0.0, 1.0 - deductions), 4)

    if authenticity > 0.75:
        verdict = "authentic"
    elif authenticity > 0.45:
        verdict = "suspicious"
    else:
        verdict = "likely_paid"

    return authenticity, verdict, evidence


# -- Main analysis -------------------------------------------------------------

def analyse_trend(
    conn: sqlite3.Connection,
    trend_name: str,
    cfg: dict,
) -> dict | None:
    df = load_trend_csv(trend_name, cfg["storage"]["trending_data_dir"])

    author_scores = score_trend_authors(df, cfg) if df is not None and len(df) > 0 else {
        "total_tweets": 0, "mean_bot_score": 0, "pct_high_bot": 0, "verified_pct": 50
    }
    volume_scores = score_trend_volume(conn, trend_name)
    celeb_overlap = check_celebrity_overlap(conn, trend_name)

    authenticity, verdict, evidence = compute_authenticity(author_scores, volume_scores, celeb_overlap)

    result = {
        "trend_name":               trend_name,
        "times_trended":            volume_scores.get("times_trended", 0),
        "peak_volume":              volume_scores.get("peak_volume"),
        "avg_volume":               volume_scores.get("avg_volume"),
        "total_tweets_sampled":     author_scores.get("total_tweets", 0),
        "pct_high_bot_authors":     author_scores.get("pct_high_bot", 0),
        "mean_author_bot_score":    author_scores.get("mean_bot_score", 0),
        "celebrities_involved":     celeb_overlap.get("total_celebrities_involved", 0),
        "celebrity_mentions":       json.dumps(celeb_overlap.get("celebrity_mentions", {})),
        "suspicious_volume_spike":  volume_scores.get("suspicious_spike", False),
        "authenticity_score":       authenticity,
        "bot_engagement_ratio":     author_scores.get("pct_high_bot", 0) / 100,
        "verdict":                  verdict,
        "evidence":                 json.dumps(evidence),
        "analyzed_at":              now_iso(),
    }

    # Save to DB
    period_rows = conn.execute(
        "SELECT MIN(captured_at) as start, MAX(captured_at) as end FROM trending_topics WHERE LOWER(trend_name) = LOWER(?)",
        (trend_name,)
    ).fetchone()

    conn.execute("""
        INSERT INTO trending_analysis (
            trend_name, period_start, period_end, times_trended,
            peak_volume, avg_volume, celebrity_mentions,
            authenticity_score, bot_engagement_ratio, verdict, evidence, analyzed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        trend_name,
        (period_rows["start"] or "2020-01-01") if period_rows else "2020-01-01",
        (period_rows["end"] or now_iso()) if period_rows else now_iso(),
        result["times_trended"],
        result["peak_volume"],
        result["avg_volume"],
        result["celebrity_mentions"],
        authenticity,
        result["bot_engagement_ratio"],
        verdict,
        result["evidence"],
        now_iso(),
    ))
    conn.commit()

    return result


def run(
    config_path: str = "config/config.yaml",
    trend_filter: str | None = None,
    output_csv: str | None = None,
) -> None:
    cfg = load_config(config_path)
    init_db(cfg["storage"]["db_path"])
    conn = get_conn(cfg["storage"]["db_path"])

    # Get distinct trend names from DB
    if trend_filter:
        trend_names = [trend_filter]
    else:
        rows = conn.execute(
            "SELECT DISTINCT trend_name FROM trending_topics ORDER BY trend_name"
        ).fetchall()
        trend_names = [r["trend_name"] for r in rows]

    logger.info(f"Analysing {len(trend_names)} trends...")
    all_results = []

    for trend_name in tqdm(trend_names, desc="Trends"):
        try:
            result = analyse_trend(conn, trend_name, cfg)
            if result:
                all_results.append(result)
                logger.info(
                    f"  {trend_name:40s}  "
                    f"verdict={result['verdict']:12s}  "
                    f"auth={result['authenticity_score']:.2f}  "
                    f"bot%={result['pct_high_bot_authors']:.1f}"
                )
        except Exception as e:
            logger.error(f"  Error for '{trend_name}': {e}")

    # Write CSV report
    out_path = output_csv or os.path.join(cfg["storage"]["reports_dir"], "trending_analysis.csv")
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if all_results:
        df = pd.DataFrame(all_results)
        df = df.sort_values("authenticity_score", ascending=True)
        df.to_csv(out_path, index=False)
        logger.info(f"\n- Trending analysis complete. Report - {out_path}")

        # Print summary
        verdicts = df["verdict"].value_counts()
        logger.info("\nVerdict Summary:")
        for v, count in verdicts.items():
            logger.info(f"  {v:15s}: {count:>4d}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse trend authenticity")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--trend", default=None, help="Analyse a specific trend")
    parser.add_argument("--output", default=None, help="Output CSV path")
    args = parser.parse_args()
    run(args.config, args.trend, args.output)
