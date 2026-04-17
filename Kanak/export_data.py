"""
export_data.py — Export processed data from SQLite to CSV and Parquet
                 files for modelling, BI tools, and further analysis.

Exports:
  processed/profiles.csv
  processed/posts.csv
  processed/replies_with_bot_scores.csv
  processed/follower_samples_with_bot_scores.csv
  processed/bot_analysis_summary.csv
  processed/trending_analysis.csv
  processed/engagement_features.csv       ← ML-ready feature matrix

Usage:
    python export_data.py
    python export_data.py --format parquet
    python export_data.py --username KimKardashian
"""

import argparse
import os
from pathlib import Path

import pandas as pd

from db import get_conn
from utils import get_logger, load_config, now_iso

logger = get_logger("exporter")


def export_table(
    conn,
    query: str,
    out_path: str,
    fmt: str = "csv",
    params: tuple = (),
) -> int:
    df = pd.read_sql_query(query, conn, params=params)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if fmt == "parquet":
        out_path = out_path.replace(".csv", ".parquet")
        df.to_parquet(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)

    logger.info(f"  - {out_path}  ({len(df):,} rows)")
    return len(df)


def build_engagement_feature_matrix(conn) -> pd.DataFrame:
    """
    Build a per-post ML feature matrix joining posts, profile, and reply signals.
    Useful for training supervised bot-engagement classifiers.
    """
    query = """
        SELECT
            p.tweet_id,
            p.user_id,
            pr.username,
            pr.profession_category,
            pr.followers_count          AS author_followers,
            pr.following_count          AS author_following,
            pr.is_verified              AS author_verified,
            p.like_count,
            p.retweet_count,
            p.reply_count,
            p.quote_count,
            p.bookmark_count,
            p.impression_count,
            p.engagement_rate,
            p.has_media,
            p.is_retweet,
            p.is_quote,
            p.is_reply,
            p.urls_count,
            p.created_at,
            -- Time features
            CAST(strftime('%H', p.created_at) AS INTEGER) AS hour_of_day,
            CAST(strftime('%w', p.created_at) AS INTEGER) AS day_of_week,
            CAST(strftime('%m', p.created_at) AS INTEGER) AS month,
            -- Reply-level bot signals (aggregated)
            COALESCE(rs.total_replies,  0)  AS scraped_reply_count,
            COALESCE(rs.avg_bot_score,  0)  AS avg_reply_bot_score,
            COALESCE(rs.pct_high_bot,   0)  AS pct_bot_replies,
            COALESCE(rs.no_pfp_pct,     0)  AS reply_no_pfp_pct,
            COALESCE(rs.no_bio_pct,     0)  AS reply_no_bio_pct
        FROM posts p
        JOIN profiles pr ON p.user_id = pr.user_id
        LEFT JOIN (
            SELECT
                parent_tweet_id,
                COUNT(*)                                        AS total_replies,
                AVG(bot_score)                                  AS avg_bot_score,
                AVG(CASE WHEN bot_score > 0.5 THEN 1.0 ELSE 0.0 END) * 100 AS pct_high_bot,
                AVG(CASE WHEN author_has_pfp = 0 THEN 1.0 ELSE 0.0 END) * 100 AS no_pfp_pct,
                AVG(CASE WHEN author_has_bio = 0 THEN 1.0 ELSE 0.0 END) * 100 AS no_bio_pct
            FROM replies
            GROUP BY parent_tweet_id
        ) rs ON rs.parent_tweet_id = p.tweet_id
        WHERE p.is_retweet = 0
        ORDER BY p.created_at DESC
    """
    return pd.read_sql_query(query, conn)


def run(
    config_path: str = "config/config.yaml",
    fmt: str = "csv",
    username_filter: str | None = None,
) -> None:
    cfg = load_config(config_path)
    conn = get_conn(cfg["storage"]["db_path"])
    out_dir = cfg["storage"]["processed_data_dir"]

    logger.info(f"Exporting data to {out_dir}/ [{fmt}]")

    user_filter_sql = ""
    params: tuple = ()
    if username_filter:
        user_filter_sql = "WHERE LOWER(pr.username) = LOWER(?)"
        params = (username_filter,)

    # 1. Profiles
    export_table(conn,
        "SELECT * FROM profiles",
        f"{out_dir}/profiles.csv", fmt)

    # 2. Posts
    post_where = f"WHERE p.user_id IN (SELECT user_id FROM profiles WHERE LOWER(username) = LOWER('{username_filter}'))" if username_filter else ""
    export_table(conn,
        f"SELECT p.*, pr.username, pr.profession_category FROM posts p JOIN profiles pr ON p.user_id = pr.user_id {post_where}",
        f"{out_dir}/posts.csv", fmt)

    # 3. Replies with bot scores
    reply_where = f"WHERE p.user_id IN (SELECT user_id FROM profiles WHERE LOWER(username) = LOWER('{username_filter}'))" if username_filter else ""
    export_table(conn,
        f"""
        SELECT r.*, p.user_id AS celebrity_user_id, pr.username AS celebrity_username
        FROM replies r
        JOIN posts p ON r.parent_tweet_id = p.tweet_id
        JOIN profiles pr ON p.user_id = pr.user_id
        {reply_where}
        """,
        f"{out_dir}/replies_with_bot_scores.csv", fmt)

    # 4. Follower samples
    fs_where = f"WHERE LOWER(pr.username) = LOWER('{username_filter}')" if username_filter else ""
    export_table(conn,
        f"""
        SELECT fs.*, pr.username AS celebrity_username
        FROM follower_samples fs
        JOIN profiles pr ON fs.target_user_id = pr.user_id
        {fs_where}
        """,
        f"{out_dir}/follower_samples_with_bot_scores.csv", fmt)

    # 5. Bot analysis summary
    export_table(conn,
        """
        SELECT ba.*, pr.username, pr.profession_category, pr.followers_count
        FROM bot_analysis ba
        JOIN profiles pr ON ba.user_id = pr.user_id
        ORDER BY ba.overall_bot_score DESC
        """,
        f"{out_dir}/bot_analysis_summary.csv", fmt)

    # 6. Trending analysis
    export_table(conn,
        "SELECT * FROM trending_analysis ORDER BY authenticity_score ASC",
        f"{out_dir}/trending_analysis.csv", fmt)

    # 7. ML feature matrix
    logger.info("  Building engagement feature matrix...")
    feat_df = build_engagement_feature_matrix(conn)
    feat_path = f"{out_dir}/engagement_features.{'parquet' if fmt == 'parquet' else 'csv'}"
    if fmt == "parquet":
        feat_df.to_parquet(feat_path, index=False)
    else:
        feat_df.to_csv(feat_path, index=False)
    logger.info(f"  - {feat_path}  ({len(feat_df):,} rows)")

    # 8. Profile snapshots (follower growth curves)
    export_table(conn,
        """
        SELECT ps.*, pr.username
        FROM profile_snapshots ps
        JOIN profiles pr ON ps.user_id = pr.user_id
        ORDER BY pr.username, ps.snapshot_at
        """,
        f"{out_dir}/follower_growth.csv", fmt)

    logger.info("\n- Export complete.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export X pipeline data to CSV/Parquet")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", dest="fmt")
    parser.add_argument("--username", default=None)
    args = parser.parse_args()
    run(args.config, args.fmt, args.username)
