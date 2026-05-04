"""
ingest.py
---------
Downloads GHArchive .json.gz files, parses events, flags bot actors,
computes per-repo and per-actor statistics, and detects coordinated activity.

Source breakdown
----------------
Foundation (Kanak)
  - download_hour / download_range / _build_url / _local_path
  - _parse_file streaming architecture
  - compute_repo_stats base signals (events_per_actor, bot_ratio, time_span_s,
    events_per_second, event_type_diversity, suspicious_score)
  - run_ingest orchestration pattern, save to Parquet

New signals (Shravani)
  - Extended bot patterns (mend-bolt, whitesource, deepsource, etc.) → now in config.py
  - _has_phish_name()          – keyword match on repo names
  - _extract_refs()            – pull branch/tag refs from CreateEvent payloads
  - _extract_ai_coauthor()     – detect AI handle co-authorship in commit messages
  - compute_actor_stats()      – per-actor behavioural fingerprint:
                                  entropy, burst fraction, suspicious_human_score
  - detect_lockstep()          – coordinated multi-account activity windows
  - compute_repo_stats extended – phish_name_flag, ai_coauthor_flag,
                                  distinct_branches, updated scoring weights
  - run_ingest returns 4-tuple  – adds df_actors, df_lockstep
  - processed_dir parameter    – supports Shravani's per-run folder layout

Merged / unified
  - All constants pulled from config.py (no more scattered hardcoding)
  - Field name: is_bot_actor (Kanak's name kept for test-suite compatibility)
  - utils.py helpers (save_parquet, ensure_dir, setup_logging) reused
"""

import gzip
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, List, Optional

import pandas as pd
import requests

from config import (
    BOT_RE,
    PHISH_RE,
    AI_HANDLES_RE,
    REPO_SUSPICION,
    ACTOR_SUSPICION,
    DEFAULT_PARAMS,
    GHARCHIVE_URL_TEMPLATE,
    PATHS,
)
from utils import setup_logging, ensure_dir, save_parquet

logger = logging.getLogger(__name__)

RAW_DIR       = PATHS.RAW
PROCESSED_DIR = PATHS.PROCESSED


# ── bot / phish helpers ───────────────────────────────────────────────────────
def _is_bot_actor(login: Optional[str]) -> bool:
    """Return True if the actor login matches any known bot pattern."""
    if not login:
        return False
    return bool(BOT_RE.search(login))


def _has_phish_name(repo_name: str) -> bool:
    """Return True if the repo's short name matches a phishing keyword."""
    name = repo_name.split("/")[-1] if "/" in repo_name else repo_name
    return bool(PHISH_RE.search(name))


# ── GHArchive download ────────────────────────────────────────────────────────
def _build_url(dt: datetime) -> str:
    return GHARCHIVE_URL_TEMPLATE.format(
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
    dt    : UTC datetime (only date + hour are used).
    force : Re-download even if the local file already exists.
    """
    dest = _local_path(dt)
    ensure_dir(dest.parent)

    if dest.exists() and not force:
        logger.info("Already cached: %s", dest.name)
        return dest

    url = _build_url(dt)
    logger.info("Downloading %s → %s", url, dest.name)
    t0 = time.time()

    with requests.get(url, stream=True, timeout=90) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=DEFAULT_PARAMS.CHUNK_SIZE):
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
    Download all GHArchive hour files between start (inclusive) and end (exclusive).
    Uses ThreadPoolExecutor for parallel downloads.
    """
    hours: List[datetime] = []
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        hours.append(cur)
        cur += timedelta(hours=1)

    paths: List[Path] = []
    with ThreadPoolExecutor(max_workers=DEFAULT_PARAMS.MAX_WORKERS) as pool:
        futures = {pool.submit(download_hour, h, force): h for h in hours}
        for fut in as_completed(futures):
            h = futures[fut]
            try:
                paths.append(fut.result())
            except Exception as exc:
                logger.error("Failed to download %s: %s", h, exc)

    return sorted(paths)


# ── payload extraction helpers (Shravani) ─────────────────────────────────────
def _extract_refs(event: dict) -> List[str]:
    """Pull branch/tag ref names from CreateEvent payloads."""
    payload = event.get("payload", {})
    ref = payload.get("ref")
    return [ref] if ref else []


def _extract_ai_coauthor(event: dict) -> bool:
    """Return True if any commit message or author name matches an AI handle."""
    commits = event.get("payload", {}).get("commits", [])
    for c in commits:
        msg    = c.get("message", "")
        author = c.get("author", {}).get("name", "")
        if AI_HANDLES_RE.search(msg) or AI_HANDLES_RE.search(author):
            return True
    return False


# ── streaming parser ──────────────────────────────────────────────────────────
def _parse_file(
    path: Path,
    max_events: Optional[int] = None,
) -> Generator[dict, None, None]:
    """
    Stream-parse a .json.gz GHArchive file.

    Yields one flat dict per event with all signals from both contributors.
    max_events=None means read all lines.
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

            login     = event.get("actor", {}).get("login")
            repo_name = event.get("repo", {}).get("name", "")

            yield {
                # ── core fields (Kanak) ────────────────────────────────────
                "event_id":    event.get("id"),
                "event_type":  event.get("type"),
                "actor_login": login,
                "actor_id":    event.get("actor", {}).get("id"),
                "repo_name":   repo_name,
                "repo_id":     event.get("repo", {}).get("id"),
                "created_at":  event.get("created_at"),
                "is_public":   event.get("public"),
                "org":         (event.get("org") or {}).get("login"),
                "payload_size": len(line),
                "is_bot_actor": _is_bot_actor(login),   # unified field name
                # ── new signals (Shravani) ─────────────────────────────────
                "phish_name":   _has_phish_name(repo_name),
                "refs":         _extract_refs(event),
                "ai_coauthor":  _extract_ai_coauthor(event),
            }


def parse_files(
    paths: List[Path],
    max_events_per_file: Optional[int] = None,
) -> pd.DataFrame:
    """Parse a list of .json.gz files into a single DataFrame."""
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


# ── per-actor stats (Shravani) ────────────────────────────────────────────────
def compute_actor_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-actor behavioural fingerprint for suspicious-human detection.

    Signals computed per actor (non-bot accounts are scored):
      - event_type entropy      (low → repetitive → suspicious)
      - burst_fraction          (fraction of inter-event gaps < 60 s)
      - total_events            (high volume flag)
      - unique_event_types == 1 (single-event-type focus)
      - ai_coauthor             (AI handle found in commit co-author)

    Each flag contributes +1 to suspicious_human_score (max 5).
    """
    from scipy.stats import entropy as scipy_entropy

    records = []
    for login, grp in df.groupby("actor_login"):
        grp = grp.sort_values("created_at")
        n   = len(grp)

        type_counts = grp["event_type"].value_counts(normalize=True)
        ev_entropy  = float(scipy_entropy(type_counts))

        times  = grp["created_at"].dropna().sort_values()
        gaps   = times.diff().dt.total_seconds().dropna()
        med_gap     = float(gaps.median()) if len(gaps) else 0.0
        min_gap     = float(gaps.min())    if len(gaps) else 0.0
        burst_frac  = float((gaps < 60).sum() / len(gaps)) if len(gaps) else 0.0

        records.append({
            "actor_login":       login,
            "total_events":      n,
            "unique_repos":      grp["repo_name"].nunique(),
            "unique_event_types":grp["event_type"].nunique(),
            "event_entropy":     ev_entropy,
            "burst_fraction":    burst_frac,
            "median_gap_s":      med_gap,
            "min_gap_s":         min_gap,
            "span_hours":        (grp["created_at"].max() - grp["created_at"].min())
                                  .total_seconds() / 3600,
            "ai_coauthor":       bool(grp["ai_coauthor"].any()),
            "is_bot_actor":      bool(grp["is_bot_actor"].all()),
        })

    actor_df = pd.DataFrame(records)
    if actor_df.empty:
        return actor_df

    # Score only non-bot humans
    human = actor_df[~actor_df["is_bot_actor"]].copy()
    human["susp_low_entropy"]  = (human["event_entropy"] < ACTOR_SUSPICION.ENTROPY_THRESHOLD).astype(int)
    human["susp_burst"]        = (human["burst_fraction"] > ACTOR_SUSPICION.BURST_THRESHOLD).astype(int)
    human["susp_high_volume"]  = (human["total_events"] > ACTOR_SUSPICION.HIGH_VOLUME).astype(int)
    human["susp_single_type"]  = (human["unique_event_types"] == 1).astype(int)
    human["susp_ai_coauthor"]  = human["ai_coauthor"].astype(int)
    human["suspicious_human_score"] = (
        human["susp_low_entropy"] +
        human["susp_burst"] +
        human["susp_high_volume"] +
        human["susp_single_type"] +
        human["susp_ai_coauthor"]
    )

    score_cols = [
        "actor_login", "suspicious_human_score",
        "susp_low_entropy", "susp_burst",
        "susp_high_volume", "susp_single_type", "susp_ai_coauthor",
    ]
    actor_df = actor_df.merge(human[score_cols], on="actor_login", how="left")
    actor_df["suspicious_human_score"] = (
        actor_df["suspicious_human_score"].fillna(0).astype(int)
    )
    return actor_df.sort_values("suspicious_human_score", ascending=False)


# ── lockstep detection (Shravani) ─────────────────────────────────────────────
def detect_lockstep(
    df: pd.DataFrame,
    window_minutes: int = DEFAULT_PARAMS.LOCKSTEP_WINDOW_MIN,
    min_accounts:   int = DEFAULT_PARAMS.LOCKSTEP_MIN_ACCOUNTS,
) -> pd.DataFrame:
    """
    Find groups of accounts that hit the same repos in tight time windows.
    A lockstep window flags coordinated or synthetic activity.
    """
    df2 = df.copy()
    df2["window"] = df2["created_at"].dt.floor(f"{window_minutes}min")

    results = []
    for (repo, window), grp in df2.groupby(["repo_name", "window"]):
        actors = grp["actor_login"].dropna().unique()
        if len(actors) >= min_accounts:
            results.append({
                "repo_name":   repo,
                "window_start": window,
                "actor_count": len(actors),
                "event_count": len(grp),
                "actors":      ",".join(sorted(actors)),
                "event_types": ",".join(sorted(grp["event_type"].unique())),
            })

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results).sort_values("actor_count", ascending=False)


# ── per-repo stats ────────────────────────────────────────────────────────────
def compute_repo_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-repo statistics and compute a suspicion score.

    Base signals (Kanak): events_per_actor, bot_ratio, time_span_s,
                          events_per_second, event_type_diversity
    New signals (Shravani): phish_name_flag, ai_coauthor_flag,
                            distinct_branches (from CreateEvents)
    Extended scoring weights from config.REPO_SUSPICION.
    """
    rs = REPO_SUSPICION

    stats = df.groupby("repo_name", sort=False).agg(
        total_events     =("event_id",    "count"),
        unique_actors    =("actor_login", "nunique"),
        bot_events       =("is_bot_actor","sum"),
        first_event      =("created_at",  "min"),
        last_event       =("created_at",  "max"),
        total_payload_b  =("payload_size","sum"),
        phish_name_flag  =("phish_name",  "max"),   # Shravani
        ai_coauthor_flag =("ai_coauthor", "max"),   # Shravani
        unique_orgs      =("org",         "nunique"),
    ).reset_index()

    stats["events_per_actor"]  = stats["total_events"] / stats["unique_actors"].replace(0, 1)
    stats["bot_ratio"]         = stats["bot_events"] / stats["total_events"]
    stats["time_span_s"]       = (
        stats["last_event"] - stats["first_event"]
    ).dt.total_seconds().fillna(0)
    stats["events_per_second"] = stats["total_events"] / stats["time_span_s"].replace(0, 1)

    # Event-type diversity
    ev_div = (
        df.groupby("repo_name")["event_type"]
        .nunique()
        .rename("event_type_diversity")
    )
    stats = stats.merge(ev_div, on="repo_name", how="left")

    # Branch explosion from CreateEvents (Shravani)
    create_df = df[df["event_type"] == "CreateEvent"].copy()
    if not create_df.empty:
        exploded = create_df.explode("refs")
        branch_counts = (
            exploded[exploded["refs"].notna()]
            .groupby("repo_name")["refs"]
            .nunique()
            .rename("distinct_branches")
        )
        stats = stats.merge(branch_counts, on="repo_name", how="left")
    else:
        stats["distinct_branches"] = 0

    stats["distinct_branches"] = stats["distinct_branches"].fillna(0).astype(int)

    # Suspicion score — Kanak's base + Shravani's new signals
    stats["suspicious_score"] = (
        (stats["events_per_actor"]  > rs.EVENTS_PER_ACTOR_THRESHOLD).astype(int) * 1
      + (stats["bot_ratio"]         > rs.BOT_RATIO_THRESHOLD).astype(int)        * 2
      + (stats["unique_actors"]    <= rs.UNIQUE_ACTORS_MIN).astype(int)           * 1
      + (stats["events_per_second"] > rs.EVENTS_PER_SECOND_THRESHOLD).astype(int)* 1
      + stats["phish_name_flag"].astype(int)                                      * rs.PHISH_NAME_WEIGHT
      + stats["ai_coauthor_flag"].astype(int)                                     * rs.AI_COAUTHOR_WEIGHT
      + (stats["distinct_branches"] > rs.BRANCH_EXPLOSION_THRESHOLD).astype(int) * rs.BRANCH_EXPLOSION_WEIGHT
    )

    return stats.sort_values("suspicious_score", ascending=False)


# ── orchestration ─────────────────────────────────────────────────────────────
def run_ingest(
    start: datetime,
    end: datetime,
    max_events_per_file: Optional[int] = None,
    force_download: bool = False,
    output_prefix: str = "events",
    processed_dir: Optional[Path] = None,   # Shravani: per-run dirs
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Full ingest pipeline: download → parse → compute stats → save Parquet.

    Returns (events_df, repo_stats_df, actor_stats_df, lockstep_df).
    actor_stats_df and lockstep_df are new outputs from Shravani.
    """
    setup_logging()
    processed_dir = Path(processed_dir) if processed_dir is not None else PROCESSED_DIR
    ensure_dir(processed_dir)

    paths = download_range(start, end, force=force_download)
    if not paths:
        logger.error("No files downloaded.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_events = parse_files(paths, max_events_per_file=max_events_per_file)
    if df_events.empty:
        return df_events, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_repos    = compute_repo_stats(df_events)
    df_actors   = compute_actor_stats(df_events)
    df_lockstep = detect_lockstep(df_events)

    save_parquet(df_events,   processed_dir / f"{output_prefix}_raw.parquet")
    save_parquet(df_repos,    processed_dir / f"{output_prefix}_repo_stats.parquet")
    save_parquet(df_actors,   processed_dir / f"{output_prefix}_actor_stats.parquet")
    if not df_lockstep.empty:
        save_parquet(df_lockstep, processed_dir / f"{output_prefix}_lockstep.parquet")

    logger.info(
        "Ingest complete. %d events | %d repos | %d actors | %d lockstep windows.",
        len(df_events), len(df_repos), len(df_actors), len(df_lockstep),
    )
    return df_events, df_repos, df_actors, df_lockstep


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    from config import DATETIME_FMT

    parser = argparse.ArgumentParser(description="Ingest GHArchive events")
    parser.add_argument("--start",      required=True)
    parser.add_argument("--end",        required=True)
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--force",      action="store_true")
    parser.add_argument("--prefix",     default="events")
    args = parser.parse_args()

    run_ingest(
        start=datetime.strptime(args.start, DATETIME_FMT),
        end=datetime.strptime(args.end,     DATETIME_FMT),
        max_events_per_file=args.max_events,
        force_download=args.force,
        output_prefix=args.prefix,
    )
