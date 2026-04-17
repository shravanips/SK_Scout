"""
pipeline.py — Master orchestrator for the X Bot Intelligence Pipeline.

Runs all stages in sequence (or selectively):
  1. init_db          — create SQLite tables
  2. scrape_profiles  — fetch celebrity profile data
  3. scrape_posts     — fetch all posts since 2020
  4. scrape_replies   — fetch comments on top posts
  5. scrape_followers — sample followers for bot analysis
  6. scrape_trending  — snapshot current trending topics
  7. bot_detector     — score followers, comments, engagement
  8. trending_analyzer— link trends to bot data, assign verdicts
  9. export_data      — dump everything to CSV

Usage:
    python pipeline.py                         # full pipeline
    python pipeline.py --stages 1,2,3          # only init + profiles + posts
    python pipeline.py --username KimKardashian # one celebrity only
    python pipeline.py --skip 5,6              # skip follower + trending scrape
    python pipeline.py --dry-run               # print plan, don't execute
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from db import init_db
from utils import get_logger, load_config, now_iso

logger = get_logger("pipeline")

STAGES = {
    1: ("init_db",            "Initialise database"),
    2: ("scrape_profiles",    "Fetch celebrity profiles"),
    3: ("scrape_posts",       "Fetch posts since 2020"),
    4: ("scrape_replies",     "Fetch replies / comments"),
    5: ("scrape_followers",   "Sample followers"),
    6: ("scrape_trending",    "Capture trending topics"),
    7: ("bot_detector",       "Run bot detection"),
    8: ("trending_analyzer",  "Analyse trend authenticity"),
    9: ("export_data",        "Export to CSV/Parquet"),
}


def run_stage(stage_num: int, cfg: dict, args: argparse.Namespace) -> bool:
    name, description = STAGES[stage_num]
    logger.info(f"\n{'='*60}")
    logger.info(f"  Stage {stage_num}: {description}")
    logger.info(f"{'='*60}")

    start = time.time()
    try:
        if stage_num == 1:
            init_db(cfg["storage"]["db_path"])

        elif stage_num == 2:
            import scrape_profiles
            scrape_profiles.run(args.config)

        elif stage_num == 3:
            import scrape_posts
            scrape_posts.run(args.config, args.username)

        elif stage_num == 4:
            import scrape_replies
            scrape_replies.run(
                args.config,
                args.username,
                posts_per_user=args.posts_per_user,
                replies_per_post=args.replies_per_post,
            )

        elif stage_num == 5:
            import scrape_followers
            scrape_followers.run(args.config, args.username, args.follower_sample_size)

        elif stage_num == 6:
            import scrape_trending
            scrape_trending.run(args.config, mode="snapshot")

        elif stage_num == 7:
            import bot_detector
            bot_detector.run(args.config, args.username)

        elif stage_num == 8:
            import trending_analyzer
            trending_analyzer.run(args.config)

        elif stage_num == 9:
            import export_data
            export_data.run(args.config, args.export_format, args.username)

        elapsed = round(time.time() - start, 1)
        logger.info(f"  [OK] Stage {stage_num} complete in {elapsed}s")
        return True

    except Exception as e:
        elapsed = round(time.time() - start, 1)
        logger.error(f"  [FAILED] Stage {stage_num} FAILED after {elapsed}s: {e}")
        if args.fail_fast:
            raise
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="X Bot Intelligence Pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stages:
  1  init_db            Initialise SQLite database
  2  scrape_profiles    Fetch celebrity profile data
  3  scrape_posts       Fetch posts since 2020-01-01
  4  scrape_replies     Fetch comments on top posts
  5  scrape_followers   Sample followers for bot analysis
  6  scrape_trending    Capture current trending topics (snapshot)
  7  bot_detector       Score followers / comments / engagement
  8  trending_analyzer  Analyse trend authenticity vs bot data
  9  export_data        Export everything to CSV / Parquet

Examples:
  python pipeline.py                              # full run
  python pipeline.py --stages 1,2,3              # only first 3 stages
  python pipeline.py --username selenagomez      # one celeb
  python pipeline.py --skip 5,6                  # skip follower+trending
        """,
    )
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    parser.add_argument("--stages", default=None, help="Comma-separated stage numbers to run (e.g. 1,2,3)")
    parser.add_argument("--skip", default=None, help="Comma-separated stage numbers to skip")
    parser.add_argument("--username", default=None, help="Limit to a single celebrity username")
    parser.add_argument("--posts-per-user", type=int, default=50, help="Posts to collect replies for (stage 4)")
    parser.add_argument("--replies-per-post", type=int, default=200, help="Replies per post (stage 4)")
    parser.add_argument("--follower-sample-size", type=int, default=1000, help="Followers to sample per celeb (stage 5)")
    parser.add_argument("--export-format", choices=["csv", "parquet"], default="csv")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first stage failure")
    parser.add_argument("--dry-run", action="store_true", help="Print execution plan without running")
    args = parser.parse_args()

    cfg = load_config(args.config)

    # Determine which stages to run
    if args.stages:
        selected = [int(s.strip()) for s in args.stages.split(",")]
    else:
        selected = list(STAGES.keys())

    if args.skip:
        skip_set = {int(s.strip()) for s in args.skip.split(",")}
        selected = [s for s in selected if s not in skip_set]

    # -- Dry run --
    if args.dry_run:
        logger.info("\n---  Execution plan:")
        for s in selected:
            logger.info(f"  [{s}] {STAGES[s][1]}")
        if args.username:
            logger.info(f"\n  Filtered to: @{args.username}")
        return

    # -- Run --
    logger.info(f"\n>>> X Bot Intelligence Pipeline")
    logger.info(f"   Started : {now_iso()}")
    logger.info(f"   Stages  : {selected}")
    logger.info(f"   DB      : {cfg['storage']['db_path']}")
    if args.username:
        logger.info(f"   User    : @{args.username}")

    results = {}
    for stage_num in selected:
        ok = run_stage(stage_num, cfg, args)
        results[stage_num] = ok

    # -- Summary --
    logger.info(f"\n{'='*60}")
    logger.info("  PIPELINE SUMMARY")
    logger.info(f"{'='*60}")
    for s, ok in results.items():
        status = "[OK]" if ok else "[FAILED]"
        logger.info(f"  {status}  [{s}] {STAGES[s][1]}")

    failed = [s for s, ok in results.items() if not ok]
    if failed:
        logger.warning(f"\n{len(failed)} stage(s) failed: {failed}")
        sys.exit(1)
    else:
        logger.info(f"\n*** All {len(results)} stages completed successfully!")


if __name__ == "__main__":
    main()
