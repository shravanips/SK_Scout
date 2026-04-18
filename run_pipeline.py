"""
run_pipeline.py  –  SK_Scout / Shravani
-----------------------------------------
One-command entry point: download → ingest → analyse → HTML report.

Usage
-----
# Quick test (5 000 events, 1 hour)
python run_pipeline.py --start "2026-04-15 12" --end "2026-04-15 13" --max-events 5000

# Standard run (4 hours)
python run_pipeline.py --start "2026-04-15 10" --end "2026-04-15 14"

# Large run (24 hours)
python run_pipeline.py --start "2026-04-15 00" --end "2026-04-16 00"
"""

import argparse, sys, logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ingest    import run_ingest
from analytics import run_analytics

DATE_FMT = "%Y-%m-%d %H"

def main():
    p = argparse.ArgumentParser(description="SK Scout pipeline – Shravani")
    p.add_argument("--start",      required=True)
    p.add_argument("--end",        required=True)
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--force",      action="store_true")
    p.add_argument("--clusters",   type=int, default=8)
    p.add_argument("--prefix",     default="events")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    start = datetime.strptime(args.start, DATE_FMT)
    end   = datetime.strptime(args.end,   DATE_FMT)

    print(f"\n{'='*60}")
    print(f"  SK Scout  |  Shravani")
    print(f"  Window : {start}  →  {end}")
    print(f"{'='*60}\n")

    df_events, df_repos, df_actors, df_lockstep = run_ingest(
        start=start, end=end,
        max_events_per_file=args.max_events,
        force_download=args.force,
        output_prefix=args.prefix,
    )

    if df_events.empty:
        print("No events ingested. Exiting."); sys.exit(1)

    print(f"\n✓ {len(df_events):,} events | "
          f"{df_events['repo_name'].nunique():,} repos | "
          f"{df_events['actor_login'].nunique():,} actors\n")

    run_analytics(events_prefix=args.prefix, n_clusters=args.clusters)

    print(f"\n✓ Report saved → data/reports/report.html\n")

if __name__ == "__main__":
    main()