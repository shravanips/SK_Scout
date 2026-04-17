"""
run_pipeline.py
---------------
Convenience entry point that runs ingest → analytics in one command.

Usage
-----
# Ingest 4 hours of data (~40k–100k events) then analyse:
python run_pipeline.py --start "2026-04-15 10" --end "2026-04-15 14"

# Large run (24 hours, ~1M events) with GitHub enrichment:
python run_pipeline.py --start "2026-04-15 00" --end "2026-04-16 00" --max-enrich 500

# Quick test on 5 000 events, no API calls:
python run_pipeline.py --start "2026-04-15 12" --end "2026-04-15 13" \
                       --max-events 5000 --no-enrich
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# allow running from project root
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ingest    import run_ingest
from analytics import run_analytics
from utils     import setup_logging

DATE_FMT = "%Y-%m-%d %H"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full GitGub pipeline: ingest GHArchive data + analyse bots & repos"
    )
    parser.add_argument("--start",      required=True,
                        help="UTC window start, e.g. '2026-04-15 10'")
    parser.add_argument("--end",        required=True,
                        help="UTC window end (exclusive), e.g. '2026-04-15 14'")
    parser.add_argument("--max-events", type=int, default=None,
                        help="Cap events per .gz file (default: unlimited)")
    parser.add_argument("--force",      action="store_true",
                        help="Re-download already cached files")
    parser.add_argument("--no-enrich",  action="store_true",
                        help="Skip GitHub API metadata enrichment")
    parser.add_argument("--max-enrich", type=int, default=300,
                        help="Max repos to enrich via GitHub API (default: 300)")
    parser.add_argument("--clusters",   type=int, default=8,
                        help="KMeans clusters for repo purpose analysis (default: 8)")
    parser.add_argument("--prefix",     default="events",
                        help="Output file prefix (default: events)")
    args = parser.parse_args()

    setup_logging()

    start = datetime.strptime(args.start, DATE_FMT)
    end   = datetime.strptime(args.end,   DATE_FMT)

    print(f"\n{'='*60}")
    print(f"  GitGub Pipeline")
    print(f"  Window : {start}  →  {end}")
    print(f"  Prefix : {args.prefix}")
    print(f"{'='*60}\n")

    # ── Step 1: ingest ────────────────────────────────────────────────────────
    df_events, df_stats = run_ingest(
        start=start,
        end=end,
        max_events_per_file=args.max_events,
        force_download=args.force,
        output_prefix=args.prefix,
    )

    if df_events.empty:
        print("No events ingested. Exiting.")
        sys.exit(1)

    print(f"\n✓ Ingested {len(df_events):,} events across {df_events['repo_name'].nunique():,} repos\n")

    # ── Step 2: analytics ─────────────────────────────────────────────────────
    run_analytics(
        events_prefix=args.prefix,
        enrich_with_github=not args.no_enrich,
        max_enrich_repos=args.max_enrich,
        n_clusters=args.clusters,
    )

    print(f"\n✓ Reports saved to data/reports/")
    print("  Open data/reports/report.html in your browser for the full dashboard.\n")


if __name__ == "__main__":
    main()
