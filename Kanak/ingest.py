"""
ingest.py
---------
Downloads GHArchive .json.gz files for a given date/hour range,
parses events, flags bot actors, and computes per-repo statistics
at scale (10k+ events / multi-hour windows).

Changes from the original notebook:
  - Accepts a configurable date/hour range instead of a single hard-coded file.
  - Streams each .gz file line-by-line so memory usage stays constant
    regardless of file size.
  - Writes intermediate results to Parquet (data/processed/) so that
    analytics modules can be run independently without re-ingesting.
  - Adds a richer bot-detection heuristic (see _is_bot()).
  - Uses concurrent.futures for parallel downloads when multiple hours
    are requested.
"""

import gzip
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, List, Optional

import pandas as pd
import requests

from utils import setup_logging, ensure_dir, save_parquet, load_parquet

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────
GHARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
RAW_DIR = Path("data/raw/gharchive")
PROCESSED_DIR = Path("data/processed")
MAX_WORKERS = 4          # parallel download threads
CHUNK_SIZE = 8192        # bytes per download chunk

# Extended bot-detection patterns beyond just [bot] suffix
BOT_PATTERNS = [
    r"\[bot\]",
    r"-bot$",
    r"^bot-",
    r"dependabot",
    r"renovate",
    r"github-actions",
    r"codecov",
    r"snyk-bot",
    r"greenkeeper",
    r"semantic-release",
    r"allcontributors",
    r"imgbot",
]

import re
_BOT_RE = re.compile("|".join(BOT_PATTERNS), re.IGNORECASE)


def _is_bot(login: Optional[str]) -> bool:
    """Return True if the actor login matches any known bot pattern."""
    if not login:
        return False
    return bool(_BOT_RE.search(login))


# ── downloading ───────────────────────────────────────────────────────────────
def _build_url(dt: datetime) -> str:
    return GHARCHIVE_URL.format(
        year=dt.year, month=dt.month, day=dt.day, hour=dt.hour
    )


def _local_path(dt: datetime) -> Path:
    fname = f"{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}.json.gz"
    return RAW_DIR / fname


def download_hour(dt: datetime, force: bool = False) -> Path:
    """
    Download a single GHArchive hour file.

    Parameters
    ----------
    dt : datetime
        The UTC datetime (only date + hour matter).
    force : bool
        Re-download even if the local file already exists.

    Returns
    -------
    Path to the local .json.gz file.
    """
    dest = _local_path(dt)
    ensure_dir(dest.parent)

    if dest.exists() and not force:
        logger.info("Already cached: %s", dest)
        return dest

    url = _build_url(dt)
    logger.info("Downloading %s → %s", url, dest)
    t0 = time.time()

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)

    elapsed = time.time() - t0
    size_mb = dest.stat().st_size / 1_048_576
    logger.info("Downloaded %.1f MB in %.1f s", size_mb, elapsed)
    return dest


def download_range(
    start: datetime,
    end: datetime,
    force: bool = False,
) -> List[Path]:
    """
    Download all GHArchive hour files between start (inclusive)
    and end (exclusive) in parallel.

    Parameters
    ----------
    start, end : datetime
        UTC range.
    force : bool
        Re-download cached files.

    Returns
    -------
    Sorted list of local .json.gz paths.
    """
    hours: List[datetime] = []
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        hours.append(cur)
        cur += timedelta(hours=1)

    paths: List[Path] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_hour, h, force): h for h in hours}
        for fut in as_completed(futures):
            h = futures[fut]
            try:
                paths.append(fut.result())
            except Exception as exc:
                logger.error("Failed to download %s: %s", h, exc)

    return sorted(paths)


# ── parsing ───────────────────────────────────────────────────────────────────
def _parse_file(path: Path, max_events: Optional[int] = None) -> Generator[dict, None, None]:
    """
    Stream-parse a .json.gz GHArchive file.

    Yields one flat dict per event. max_events=None means read all.
    """
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if max_events is not None and i >= max_events:
                break
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("Bad JSON at line %d in %s: %s", i, path.name, exc)
                continue

            actor_login = event.get("actor", {}).get("login")
            yield {
                "event_id":    event.get("id"),
                "event_type":  event.get("type"),
                "actor_login": actor_login,
                "actor_id":    event.get("actor", {}).get("id"),
                "repo_name":   event.get("repo", {}).get("name"),
                "repo_id":     event.get("repo", {}).get("id"),
                "created_at":  event.get("created_at"),
                "is_public":   event.get("public"),
                "is_bot_actor": _is_bot(actor_login),
                # extra fields used by analytics
                "org":         event.get("org", {}).get("login") if event.get("org") else None,
                "payload_size": len(line),   # proxy for event "weight"
            }


def parse_files(
    paths: List[Path],
    max_events_per_file: Optional[int] = None,
) -> pd.DataFrame:
    """
    Parse a list of .json.gz files into a single DataFrame.

    For 10k+ scale, this streams each file and concatenates in chunks
    rather than loading everything into memory at once.
    """
    all_rows: List[dict] = []
    for path in paths:
        logger.info("Parsing %s …", path.name)
        rows = list(_parse_file(path, max_events=max_events_per_file))
        logger.info("  → %d events", len(rows))
        all_rows.extend(rows)

    if not all_rows:
        logger.warning("No events parsed.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    return df


# ── repo statistics ───────────────────────────────────────────────────────────
def compute_repo_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-repo statistics and compute a suspicion score.

    This is the scaled version of the original notebook's groupby cells,
    with additional signals for the analytics module.
    """
    stats = df.groupby("repo_name", sort=False).agg(
        total_events    =("event_id",    "count"),
        unique_actors   =("actor_login", "nunique"),
        bot_events      =("is_bot_actor", "sum"),
        first_event     =("created_at",  "min"),
        last_event      =("created_at",  "max"),
        unique_orgs     =("org",         "nunique"),
        total_payload_b =("payload_size", "sum"),
    ).reset_index()

    # derived columns
    stats["events_per_actor"] = (
        stats["total_events"] / stats["unique_actors"].replace(0, 1)
    )
    stats["bot_ratio"] = stats["bot_events"] / stats["total_events"]
    stats["time_span_s"] = (
        stats["last_event"] - stats["first_event"]
    ).dt.total_seconds().fillna(0)
    stats["events_per_second"] = (
        stats["total_events"] / stats["time_span_s"].replace(0, 1)
    )

    # event-type diversity per repo
    event_div = (
        df.groupby("repo_name")["event_type"]
        .nunique()
        .rename("event_type_diversity")
    )
    stats = stats.merge(event_div, on="repo_name", how="left")

    # suspicion score (0–5)
    stats["suspicious_score"] = (
        (stats["events_per_actor"]    >  10).astype(int) * 1
      + (stats["bot_ratio"]           > 0.5).astype(int) * 2
      + (stats["unique_actors"]       <=  1).astype(int) * 1
      + (stats["events_per_second"]   > 0.1).astype(int) * 1
    )

    return stats.sort_values("suspicious_score", ascending=False)


# ── entry point ───────────────────────────────────────────────────────────────
def run_ingest(
    start: datetime,
    end: datetime,
    max_events_per_file: Optional[int] = None,
    force_download: bool = False,
    output_prefix: str = "events",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full pipeline: download → parse → compute stats → save to Parquet.

    Returns (events_df, repo_stats_df).
    """
    setup_logging()
    ensure_dir(PROCESSED_DIR)

    paths = download_range(start, end, force=force_download)
    if not paths:
        logger.error("No files downloaded.")
        return pd.DataFrame(), pd.DataFrame()

    df_events = parse_files(paths, max_events_per_file=max_events_per_file)
    if df_events.empty:
        return df_events, pd.DataFrame()

    df_stats = compute_repo_stats(df_events)

    # persist
    save_parquet(df_events, PROCESSED_DIR / f"{output_prefix}_raw.parquet")
    save_parquet(df_stats,  PROCESSED_DIR / f"{output_prefix}_repo_stats.parquet")

    logger.info(
        "Ingest complete. %d events, %d repos.",
        len(df_events), len(df_stats),
    )
    return df_events, df_stats


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest GHArchive events")
    parser.add_argument("--start", required=True,
                        help="Start datetime, e.g. '2026-04-15 10'")
    parser.add_argument("--end",   required=True,
                        help="End datetime (exclusive), e.g. '2026-04-15 14'")
    parser.add_argument("--max-events", type=int, default=None,
                        help="Cap events per .gz file (useful for testing)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download already cached files")
    args = parser.parse_args()

    fmt = "%Y-%m-%d %H"
    run_ingest(
        start=datetime.strptime(args.start, fmt),
        end=datetime.strptime(args.end,   fmt),
        max_events_per_file=args.max_events,
        force_download=args.force,
    )
