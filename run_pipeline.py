"""
run_pipeline.py  –  SK_Scout / Shravani
-----------------------------------------
One-command entry point: download → ingest → analyse → HTML report.

Examples
--------
# Quick sanity test
python run_pipeline.py --run-tag sanity --start "2026-04-16 12" --end "2026-04-16 13" --max-events 5000

# 4-hour run
python run_pipeline.py --run-tag fourhour --start "2026-04-16 00" --end "2026-04-16 04"

# Full-day run
python run_pipeline.py --run-tag full24h --start "2026-04-16 00" --end "2026-04-16 23"
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from ingest import run_ingest
from analytics import run_analytics

DATE_FMT = "%Y-%m-%d %H"


def setup_run_logging(log_file: Path) -> None:
    """Configure console + per-run file logging."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Clear old handlers so repeated runs do not duplicate logs
    if root.handlers:
        for handler in root.handlers[:]:
            root.removeHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(formatter)

    root.addHandler(console_handler)
    root.addHandler(file_handler)


def make_run_name(run_tag: str, start: datetime, end: datetime) -> str:
    now_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_str = start.strftime("%Y%m%d_%H")
    end_str = end.strftime("%Y%m%d_%H")
    safe_tag = run_tag.strip().replace(" ", "_")
    return f"{safe_tag}_{start_str}_to_{end_str}_{now_ts}"


def save_run_info(run_dir: Path, info: dict) -> None:
    path = run_dir / "run_info.json"
    path.write_text(json.dumps(info, indent=2), encoding="utf-8")


def main():
    p = argparse.ArgumentParser(description="SK Scout pipeline – Shravani")
    p.add_argument("--start", required=True, help='e.g. "2026-04-16 00"')
    p.add_argument("--end", required=True, help='e.g. "2026-04-16 23"')
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--force", action="store_true")
    p.add_argument("--clusters", type=int, default=8)
    p.add_argument("--prefix", default="events", help="File prefix inside processed outputs")
    p.add_argument("--run-tag", default="run", help="Short label like sanity, fourhour, full24h")
    args = p.parse_args()

    start = datetime.strptime(args.start, DATE_FMT)
    end = datetime.strptime(args.end, DATE_FMT)

    # Per-run folder structure
    base_runs_dir = Path("data/runs")
    run_name = make_run_name(args.run_tag, start, end)
    run_dir = base_runs_dir / run_name
    processed_dir = run_dir / "processed"
    reports_dir = run_dir / "reports"
    logs_dir = run_dir / "logs"

    processed_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / "run.log"
    setup_run_logging(log_file)

    logging.info("Starting SK Scout pipeline")
    logging.info("Run name: %s", run_name)
    logging.info("Window: %s -> %s", start, end)
    logging.info("Max events per file: %s", args.max_events)
    logging.info("Clusters: %d", args.clusters)
    logging.info("Prefix: %s", args.prefix)

    print(f"\n{'='*72}")
    print("  SK Scout  |  Shravani")
    print(f"  Run    : {run_name}")
    print(f"  Window : {start}  →  {end}")
    print(f"{'='*72}\n")

    df_events, df_repos, df_actors, df_lockstep = run_ingest(
        start=start,
        end=end,
        max_events_per_file=args.max_events,
        force_download=args.force,
        output_prefix=args.prefix,
        processed_dir=processed_dir,   # NEW
    )

    if df_events.empty:
        logging.error("No events ingested. Exiting.")
        print("No events ingested. Exiting.")
        sys.exit(1)

    event_count = len(df_events)
    repo_count = df_events["repo_name"].nunique()
    actor_count = df_events["actor_login"].nunique()
    lockstep_count = len(df_lockstep) if df_lockstep is not None else 0

    logging.info("Ingest summary: %d events | %d repos | %d actors | %d lockstep windows",
                 event_count, repo_count, actor_count, lockstep_count)

    print(f"\n✓ {event_count:,} events | {repo_count:,} repos | {actor_count:,} actors\n")

    run_analytics(
        events_prefix=args.prefix,
        n_clusters=args.clusters,
        processed_dir=processed_dir,   # NEW
        reports_dir=reports_dir,       # NEW
    )

    run_info = {
        "run_name": run_name,
        "run_tag": args.run_tag,
        "start": args.start,
        "end": args.end,
        "max_events_per_file": args.max_events,
        "force_download": args.force,
        "clusters": args.clusters,
        "prefix": args.prefix,
        "event_count": int(event_count),
        "repo_count": int(repo_count),
        "actor_count": int(actor_count),
        "lockstep_count": int(lockstep_count),
        "processed_dir": str(processed_dir),
        "reports_dir": str(reports_dir),
        "log_file": str(log_file),
    }
    save_run_info(run_dir, run_info)

    logging.info("Run complete")
    logging.info("Report saved to %s", reports_dir / "report.html")

    print(f"\n✓ Report saved → {reports_dir / 'report.html'}")
    print(f"✓ Log saved    → {log_file}")
    print(f"✓ Run info     → {run_dir / 'run_info.json'}\n")


if __name__ == "__main__":
    main()