"""
run_pipeline.py
---------------
One-command entry point: download → ingest → analyse → HTML report.

Examples
--------
# Quick sanity test (1 hour, 5k events, no API)
python run_pipeline.py \\
  --run-tag sanity \\
  --start "2026-04-16 12" --end "2026-04-16 13" \\
  --max-events 5000 --no-enrich

# 4-hour run with GitHub API enrichment
python run_pipeline.py \\
  --run-tag fourhour \\
  --start "2026-04-16 00" --end "2026-04-16 04"

# Full-day run
python run_pipeline.py \\
  --run-tag full24h \\
  --start "2026-04-16 00" --end "2026-04-16 23" \\
  --max-enrich 500
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from ingest    import run_ingest
from analytics import run_analytics
from config    import DATETIME_FMT, DEFAULT_PARAMS, PATHS


# ── logging setup 
def setup_run_logging(log_file: Path) -> None:
    """Configure console + per-run file logging. Clears old handlers."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for handler in root.handlers[:]:
        root.removeHandler(handler)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(log_file, mode="w")
    fh.setFormatter(fmt)
    root.addHandler(ch)
    root.addHandler(fh)


# ── run naming 
def make_run_name(run_tag: str, start: datetime, end: datetime) -> str:
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_str = start.strftime("%Y%m%d_%H")
    end_str   = end.strftime("%Y%m%d_%H")
    safe_tag  = run_tag.strip().replace(" ", "_")
    return f"{safe_tag}_{start_str}_to_{end_str}_{ts}"


def save_run_info(run_dir: Path, info: dict) -> None:
    (run_dir / "run_info.json").write_text(
        json.dumps(info, indent=2), encoding="utf-8"
    )


# ── main 
def main() -> None:
    parser = argparse.ArgumentParser(
        description="GitGub full pipeline: ingest + analytics"
    )
    # timing
    parser.add_argument("--start",      required=True,
                        help='UTC window start, e.g. "2026-04-16 00"')
    parser.add_argument("--end",        required=True,
                        help='UTC window end (exclusive), e.g. "2026-04-16 04"')
    # ingest controls
    parser.add_argument("--max-events", type=int, default=None,
                        help="Cap events per .gz file (useful for testing)")
    parser.add_argument("--force",      action="store_true",
                        help="Re-download already cached files")
    parser.add_argument("--prefix",     default="events",
                        help="Parquet file prefix (default: events)")
    # analytics controls 
    parser.add_argument("--no-enrich",  action="store_true",
                        help="Skip GitHub API metadata enrichment")
    parser.add_argument("--max-enrich", type=int,
                        default=DEFAULT_PARAMS.MAX_ENRICH_REPOS,
                        help=f"Max repos to enrich via GitHub API (default: {DEFAULT_PARAMS.MAX_ENRICH_REPOS})")
    parser.add_argument("--clusters",   type=int,
                        default=DEFAULT_PARAMS.N_CLUSTERS,
                        help=f"KMeans clusters for repo purpose (default: {DEFAULT_PARAMS.N_CLUSTERS})")
    # run identity 
    parser.add_argument("--run-tag",    default="run",
                        help='Short label for this run, e.g. sanity, fourhour, full24h')
    args = parser.parse_args()

    start = datetime.strptime(args.start, DATETIME_FMT)
    end   = datetime.strptime(args.end,   DATETIME_FMT)

    # Per-run directories 
    run_name      = make_run_name(args.run_tag, start, end)
    run_dir       = PATHS.RUNS / run_name
    processed_dir = run_dir / "processed"
    reports_dir   = run_dir / "reports"
    logs_dir      = run_dir / "logs"

    for d in (processed_dir, reports_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    setup_run_logging(logs_dir / "run.log")

    print(f"\n{'='*72}")
    print("  GitGub Pipeline")
    print(f"  Run    : {run_name}")
    print(f"  Window : {start}  →  {end}")
    print(f"  Enrich : {'no' if args.no_enrich else f'yes (max {args.max_enrich} repos)'}")
    print(f"{'='*72}\n")

    # ── Step 1: ingest 
    df_events, df_repos, df_actors, df_lockstep = run_ingest(
        start=start,
        end=end,
        max_events_per_file=args.max_events,
        force_download=args.force,
        output_prefix=args.prefix,
        processed_dir=processed_dir,
    )

    if df_events.empty:
        logging.error("No events ingested. Exiting.")
        sys.exit(1)

    event_count    = len(df_events)
    repo_count     = df_events["repo_name"].nunique()
    actor_count    = df_events["actor_login"].nunique()
    lockstep_count = len(df_lockstep) if df_lockstep is not None else 0

    print(
        f"\n✓ {event_count:,} events | {repo_count:,} repos | "
        f"{actor_count:,} actors | {lockstep_count:,} lockstep windows\n"
    )

    # ── Step 2: analytics 
    run_analytics(
        events_prefix=args.prefix,
        enrich_with_github=not args.no_enrich,
        max_enrich_repos=args.max_enrich,
        n_clusters=args.clusters,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
    )

    # ── Persist run metadata 
    save_run_info(run_dir, {
        "run_name":            run_name,
        "run_tag":             args.run_tag,
        "start":               args.start,
        "end":                 args.end,
        "max_events_per_file": args.max_events,
        "force_download":      args.force,
        "enrich_with_github":  not args.no_enrich,
        "max_enrich_repos":    args.max_enrich,
        "clusters":            args.clusters,
        "prefix":              args.prefix,
        "event_count":         int(event_count),
        "repo_count":          int(repo_count),
        "actor_count":         int(actor_count),
        "lockstep_count":      int(lockstep_count),
        "processed_dir":       str(processed_dir),
        "reports_dir":         str(reports_dir),
        "log_file":            str(logs_dir / "run.log"),
    })

    print(f"\n✓ Report  → {reports_dir / 'report.html'}")
    print(f"✓ Log     → {logs_dir / 'run.log'}")
    print(f"✓ Run info → {run_dir / 'run_info.json'}\n")


if __name__ == "__main__":
    main()
