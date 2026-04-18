"""
ingest.py  –  SK_Scout / Shravani
----------------------------------
Downloads GHArchive .json.gz files, parses events, flags bots AND
suspicious human accounts, computes per-repo and per-actor statistics.

New signals beyond Kanak's work
--------------------------------
* Suspicious HUMAN detection  – accounts that look like bots but have no
  [bot] tag (low entropy, burst activity, single-event-type focus).
* Lockstep detection           – groups of accounts hitting the same repos
  in tight time windows, across ALL event types (not just stars).
* Account age vs activity      – disproportionate volume relative to how
  new the account appears in the data window.
* Repo name phishing patterns  – keyword matching inspired by StarScout
  (crack, free, wallet, hack, bot, stealer, …).
* Branch explosion flag        – repos with suspiciously many distinct
  ref names in CreateEvents (e.g. 2 000 branches).
* Multi-collaborator anomaly   – single-actor repos where a known AI/bot
  handle appears as a co-author in commit payloads.
"""

import gzip, json, logging, re, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── directories ────────────────────────────────────────────────────────────────
RAW_DIR       = Path("data/raw/gharchive")
PROCESSED_DIR = Path("data/processed")
GHARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
MAX_WORKERS   = 4
CHUNK_SIZE    = 8192

# ── known-bot regex (Kanak's list + more) ─────────────────────────────────────
_KNOWN_BOT_PATTERNS = [
    r"\[bot\]", r"-bot$", r"^bot-", r"dependabot", r"renovate",
    r"github-actions", r"codecov", r"snyk-bot", r"greenkeeper",
    r"semantic-release", r"allcontributors", r"imgbot", r"mend-bolt",
    r"whitesource", r"deepsource", r"codeclimate", r"crowdin",
    r"transifex", r"lokalise", r"travis",
]
_KNOWN_BOT_RE = re.compile("|".join(_KNOWN_BOT_PATTERNS), re.IGNORECASE)

# ── phishing / suspicious repo-name keywords (inspired by StarScout) ──────────
_PHISH_KEYWORDS = [
    "crack", "cracked", "free", "hack", "cheat", "stealer", "wallet",
    "crypto", "bot", "autoclicker", "executor", "solana", "roblox",
    "adobe", "activation", "keygen", "nulled", "leaked", "bypass",
    "spoofer", "rat", "trojan", "grabber", "logger",
]
_PHISH_RE = re.compile("|".join(_PHISH_KEYWORDS), re.IGNORECASE)

# ── AI-handle patterns (for multi-collaborator anomaly) ───────────────────────
_AI_HANDLES_RE = re.compile(
    r"claude|copilot|chatgpt|openai|gpt-?4|gemini", re.IGNORECASE
)


# ── helpers ───────────────────────────────────────────────────────────────────
def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def _is_known_bot(login: Optional[str]) -> bool:
    if not login:
        return False
    return bool(_KNOWN_BOT_RE.search(login))

def _has_phish_name(repo_name: str) -> bool:
    name = repo_name.split("/")[-1] if "/" in repo_name else repo_name
    return bool(_PHISH_RE.search(name))


# ── download ──────────────────────────────────────────────────────────────────
def _local_path(dt: datetime) -> Path:
    return RAW_DIR / f"{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}.json.gz"

def download_hour(dt: datetime, force: bool = False) -> Path:
    dest = _local_path(dt)
    _ensure_dir(dest.parent)
    if dest.exists() and not force:
        logger.info("Cached: %s", dest.name)
        return dest
    url = GHARCHIVE_URL.format(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)
    logger.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=90) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(CHUNK_SIZE):
                f.write(chunk)
    logger.info("Saved %.1f MB → %s", dest.stat().st_size / 1e6, dest.name)
    return dest

def download_range(start: datetime, end: datetime, force: bool = False) -> List[Path]:
    hours, cur = [], start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        hours.append(cur); cur += timedelta(hours=1)
    paths = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(download_hour, h, force): h for h in hours}
        for fut in as_completed(futs):
            try:
                paths.append(fut.result())
            except Exception as e:
                logger.error("Download failed %s: %s", futs[fut], e)
    return sorted(paths)


# ── parse ─────────────────────────────────────────────────────────────────────
def _extract_refs(event: dict) -> List[str]:
    """Pull branch/tag ref names from CreateEvent payloads."""
    payload = event.get("payload", {})
    ref = payload.get("ref")
    return [ref] if ref else []

def _extract_ai_coauthor(event: dict) -> bool:
    """Check commit messages/authors for AI handle co-authorship."""
    commits = event.get("payload", {}).get("commits", [])
    for c in commits:
        msg = c.get("message", "")
        author = c.get("author", {}).get("name", "")
        if _AI_HANDLES_RE.search(msg) or _AI_HANDLES_RE.search(author):
            return True
    return False

def _parse_file(path: Path, max_events: Optional[int] = None) -> Generator[dict, None, None]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if max_events is not None and i >= max_events:
                break
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue

            login     = ev.get("actor", {}).get("login")
            repo_name = ev.get("repo", {}).get("name", "")
            ev_type   = ev.get("type", "")

            yield {
                "event_id":        ev.get("id"),
                "event_type":      ev_type,
                "actor_login":     login,
                "actor_id":        ev.get("actor", {}).get("id"),
                "repo_name":       repo_name,
                "repo_id":         ev.get("repo", {}).get("id"),
                "created_at":      ev.get("created_at"),
                "is_public":       ev.get("public"),
                "org":             (ev.get("org") or {}).get("login"),
                "payload_size":    len(line),
                # ── bot flags ──────────────────────────────────────────────
                "is_known_bot":    _is_known_bot(login),
                # ── new signals ────────────────────────────────────────────
                "phish_name":      _has_phish_name(repo_name),
                "refs":            _extract_refs(ev),          # for branch explosion
                "ai_coauthor":     _extract_ai_coauthor(ev),   # AI handle in commits
            }

def parse_files(paths: List[Path], max_events_per_file: Optional[int] = None) -> pd.DataFrame:
    rows = []
    for p in paths:
        logger.info("Parsing %s …", p.name)
        batch = list(_parse_file(p, max_events_per_file))
        logger.info("  → %d events", len(batch))
        rows.extend(batch)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    return df


# ── per-actor stats (NEW) ──────────────────────────────────────────────────────
def compute_actor_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-actor behavioral fingerprint.
    Flags suspicious HUMAN accounts using:
      - event_type entropy  (real humans diversify; bots repeat one type)
      - burst score         (events in tight time windows)
      - repo diversity      (bots often hit many repos or exactly 1)
      - AI co-author flag
    """
    from scipy.stats import entropy as scipy_entropy
    import numpy as np

    records = []
    for login, grp in df.groupby("actor_login"):
        grp = grp.sort_values("created_at")
        n   = len(grp)

        # event-type entropy
        type_counts = grp["event_type"].value_counts(normalize=True)
        ev_entropy  = float(scipy_entropy(type_counts))

        # inter-event gap (seconds) – median & min
        times   = grp["created_at"].dropna().sort_values()
        gaps    = times.diff().dt.total_seconds().dropna()
        med_gap = float(gaps.median()) if len(gaps) else 0.0
        min_gap = float(gaps.min())    if len(gaps) else 0.0

        # burst: fraction of events within 60 s of previous
        burst_frac = float((gaps < 60).sum() / len(gaps)) if len(gaps) else 0.0

        unique_repos  = grp["repo_name"].nunique()
        unique_types  = grp["event_type"].nunique()
        ai_coauthor   = bool(grp["ai_coauthor"].any())
        is_known_bot  = bool(grp["is_known_bot"].all())

        # activity span in hours
        span_h = (
            (grp["created_at"].max() - grp["created_at"].min()).total_seconds() / 3600
        )

        records.append({
            "actor_login":    login,
            "total_events":   n,
            "unique_repos":   unique_repos,
            "unique_event_types": unique_types,
            "event_entropy":  ev_entropy,
            "burst_fraction": burst_frac,
            "median_gap_s":   med_gap,
            "min_gap_s":      min_gap,
            "span_hours":     span_h,
            "ai_coauthor":    ai_coauthor,
            "is_known_bot":   is_known_bot,
        })

    actor_df = pd.DataFrame(records)
    if actor_df.empty:
        return actor_df

    # ── suspicious HUMAN score ─────────────────────────────────────────────
    # (only for non-known-bots)
    human = actor_df[~actor_df["is_known_bot"]].copy()
    human["susp_low_entropy"]   = (human["event_entropy"]  < 0.5).astype(int)
    human["susp_burst"]         = (human["burst_fraction"] > 0.6).astype(int)
    human["susp_high_volume"]   = (human["total_events"]   > 20 ).astype(int)
    human["susp_single_type"]   = (human["unique_event_types"] == 1).astype(int)
    human["susp_ai_coauthor"]   = human["ai_coauthor"].astype(int)
    human["suspicious_human_score"] = (
        human["susp_low_entropy"] +
        human["susp_burst"]       +
        human["susp_high_volume"] +
        human["susp_single_type"] +
        human["susp_ai_coauthor"]
    )
    actor_df = actor_df.merge(
        human[["actor_login", "suspicious_human_score",
               "susp_low_entropy", "susp_burst",
               "susp_high_volume", "susp_single_type", "susp_ai_coauthor"]],
        on="actor_login", how="left"
    )
    actor_df["suspicious_human_score"] = actor_df["suspicious_human_score"].fillna(0).astype(int)
    return actor_df.sort_values("suspicious_human_score", ascending=False)


# ── lockstep detection (NEW) ──────────────────────────────────────────────────
def detect_lockstep(df: pd.DataFrame, window_minutes: int = 30, min_accounts: int = 3,
                    min_repos: int = 2) -> pd.DataFrame:
    """
    Find groups of accounts that repeatedly hit the same repos
    in tight time windows – across ALL event types, not just stars.

    Returns a DataFrame of (repo_name, window_start, actors, event_count).
    """
    df2 = df.copy()
    df2["window"] = df2["created_at"].dt.floor(f"{window_minutes}min")
    grouped = df2.groupby(["repo_name", "window"])

    results = []
    for (repo, window), grp in grouped:
        actors = grp["actor_login"].dropna().unique()
        if len(actors) >= min_accounts:
            results.append({
                "repo_name":    repo,
                "window_start": window,
                "actor_count":  len(actors),
                "event_count":  len(grp),
                "actors":       ",".join(sorted(actors)),
                "event_types":  ",".join(sorted(grp["event_type"].unique())),
            })

    if not results:
        return pd.DataFrame()

    lockstep_df = pd.DataFrame(results)
    # cross-repo: how many distinct repos each actor cluster hits
    return lockstep_df.sort_values("actor_count", ascending=False)


# ── per-repo stats ─────────────────────────────────────────────────────────────
def compute_repo_stats(df: pd.DataFrame) -> pd.DataFrame:
    stats = df.groupby("repo_name", sort=False).agg(
        total_events   =("event_id",       "count"),
        unique_actors  =("actor_login",    "nunique"),
        bot_events     =("is_known_bot",   "sum"),
        first_event    =("created_at",     "min"),
        last_event     =("created_at",     "max"),
        phish_name_flag=("phish_name",     "max"),
        ai_coauthor_flag=("ai_coauthor",   "max"),
        total_payload_b=("payload_size",   "sum"),
    ).reset_index()

    stats["events_per_actor"] = stats["total_events"] / stats["unique_actors"].replace(0, 1)
    stats["bot_ratio"]        = stats["bot_events"]   / stats["total_events"]
    stats["time_span_s"]      = (
        stats["last_event"] - stats["first_event"]
    ).dt.total_seconds().fillna(0)
    stats["events_per_second"] = stats["total_events"] / stats["time_span_s"].replace(0, 1)

    # event-type diversity per repo
    ev_div = df.groupby("repo_name")["event_type"].nunique().rename("event_type_diversity")
    stats  = stats.merge(ev_div, on="repo_name", how="left")

    # branch explosion: count distinct refs per repo (from CreateEvents)
    create_df = df[df["event_type"] == "CreateEvent"].copy()
    if not create_df.empty:
        create_df = create_df.explode("refs")
        branch_counts = (
            create_df[create_df["refs"].notna()]
            .groupby("repo_name")["refs"].nunique()
            .rename("distinct_branches")
        )
        stats = stats.merge(branch_counts, on="repo_name", how="left")
    else:
        stats["distinct_branches"] = 0
    stats["distinct_branches"] = stats["distinct_branches"].fillna(0).astype(int)

    # ── composite suspicion score ──────────────────────────────────────────
    stats["suspicious_score"] = (
        (stats["events_per_actor"]   >  10 ).astype(int) * 1 +
        (stats["bot_ratio"]          > 0.5 ).astype(int) * 2 +
        (stats["unique_actors"]      <=  1 ).astype(int) * 1 +
        (stats["events_per_second"]  > 0.1 ).astype(int) * 1 +
        stats["phish_name_flag"].astype(int)               * 3 +  # NEW
        stats["ai_coauthor_flag"].astype(int)              * 2 +  # NEW
        (stats["distinct_branches"]  > 100 ).astype(int)  * 2    # NEW branch explosion
    )

    return stats.sort_values("suspicious_score", ascending=False)


# ── full pipeline ──────────────────────────────────────────────────────────────
def run_ingest(
    start: datetime,
    end: datetime,
    max_events_per_file: Optional[int] = None,
    force_download: bool = False,
    output_prefix: str = "events",
) -> tuple:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    _ensure_dir(PROCESSED_DIR)

    paths     = download_range(start, end, force=force_download)
    if not paths:
        logger.error("No files downloaded."); return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_events = parse_files(paths, max_events_per_file)
    if df_events.empty:
        return df_events, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_repos    = compute_repo_stats(df_events)
    df_actors   = compute_actor_stats(df_events)
    df_lockstep = detect_lockstep(df_events)

    _ensure_dir(PROCESSED_DIR)
    df_events.to_parquet(PROCESSED_DIR / f"{output_prefix}_raw.parquet",       index=False)
    df_repos.to_parquet (PROCESSED_DIR / f"{output_prefix}_repo_stats.parquet",index=False)
    df_actors.to_parquet(PROCESSED_DIR / f"{output_prefix}_actor_stats.parquet",index=False)
    if not df_lockstep.empty:
        df_lockstep.to_parquet(PROCESSED_DIR / f"{output_prefix}_lockstep.parquet", index=False)

    logger.info("Ingest done. %d events | %d repos | %d actors | %d lockstep windows",
                len(df_events), len(df_repos), len(df_actors), len(df_lockstep))
    return df_events, df_repos, df_actors, df_lockstep


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start",      required=True, help="e.g. '2026-04-15 10'")
    p.add_argument("--end",        required=True, help="e.g. '2026-04-15 14'")
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--force",      action="store_true")
    p.add_argument("--prefix",     default="events")
    args = p.parse_args()
    fmt  = "%Y-%m-%d %H"
    run_ingest(datetime.strptime(args.start, fmt), datetime.strptime(args.end, fmt),
               args.max_events, args.force, args.prefix)