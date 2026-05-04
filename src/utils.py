"""
utils.py
--------
Shared helpers used by ingest.py, analytics.py, and notebooks.

Source breakdown
----------------
All code here is Kanak's original utils.py.
Constants (token, log level) now read from config.py instead of being
inlined, so changes propagate automatically.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from config import GITHUB_TOKEN, LOG_LEVEL


# ── logging ───────────────────────────────────────────────────────────────────
def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with timestamp + level formatting."""
    from pathlib import Path as _Path
    log_dir = _Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "run.log", mode="a"),
        ],
    )


# ── filesystem ────────────────────────────────────────────────────────────────
def ensure_dir(path: Path) -> Path:
    """Create directory (and parents) if it doesn't exist. Return the path."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# ── Parquet helpers ───────────────────────────────────────────────────────────
def save_parquet(df: pd.DataFrame, path: Path, **kwargs) -> None:
    """Save a DataFrame to Parquet, creating parent dirs as needed."""
    ensure_dir(Path(path).parent)
    df.to_parquet(path, index=False, **kwargs)
    size_kb = Path(path).stat().st_size / 1024
    logging.getLogger(__name__).info(
        "Saved %s  (%.1f KB, %d rows)", path, size_kb, len(df)
    )


def load_parquet(path: Path) -> pd.DataFrame:
    """Load a Parquet file, raising a clear error if it doesn't exist."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {path}\n"
            "Run `python src/ingest.py` first to generate processed data."
        )
    return pd.read_parquet(path)


# ── GitHub API helpers ────────────────────────────────────────────────────────
_GH_API_BASE = "https://api.github.com"
_GH_TOKEN = GITHUB_TOKEN


def _gh_headers() -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if _GH_TOKEN:
        h["Authorization"] = f"Bearer {_GH_TOKEN}"
    return h


def fetch_repo_metadata(repo_full_name: str, retries: int = 3) -> Optional[dict]:
    """
    Fetch repository metadata from the GitHub REST API.

    Returns a dict with fields: description, language, topics,
    stargazers_count, forks_count, size, archived, created_at, pushed_at.
    Returns None on error.

    NOTE: Requires GITHUB_TOKEN env var for higher rate limits (5000/hr vs 60/hr).
    """
    url = f"{_GH_API_BASE}/repos/{repo_full_name}"
    log = logging.getLogger(__name__)

    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=_gh_headers(), timeout=15)
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                reset_ts = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(0, reset_ts - int(time.time())) + 1
                log.warning("Rate limited. Sleeping %d s …", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return {
                "description":      data.get("description", ""),
                "language":         data.get("language", ""),
                "topics":           data.get("topics", []),
                "stargazers_count": data.get("stargazers_count", 0),
                "forks_count":      data.get("forks_count", 0),
                "size_kb":          data.get("size", 0),
                "archived":         data.get("archived", False),
                "created_at":       data.get("created_at", ""),
                "pushed_at":        data.get("pushed_at", ""),
                "default_branch":   data.get("default_branch", ""),
                "open_issues":      data.get("open_issues_count", 0),
                "license":          (data.get("license") or {}).get("spdx_id", ""),
                "has_readme":       True,
            }
        except requests.RequestException as exc:
            log.warning(
                "Attempt %d/%d for %s failed: %s",
                attempt + 1, retries, repo_full_name, exc,
            )
            time.sleep(2 ** attempt)
    return None


def enrich_repos_with_github_api(
    repo_names: list,
    max_repos: int = 500,
    delay_s: float = 0.1,
) -> pd.DataFrame:
    """
    Fetch GitHub API metadata for up to `max_repos` repos.
    Respects rate limits automatically.

    Returns a DataFrame with one row per repo.
    """
    log = logging.getLogger(__name__)
    results = []
    for i, name in enumerate(repo_names[:max_repos]):
        if i > 0 and i % 100 == 0:
            log.info("Enriched %d/%d repos …", i, min(len(repo_names), max_repos))
        meta = fetch_repo_metadata(name)
        if meta:
            meta["repo_name"] = name
            results.append(meta)
        time.sleep(delay_s)

    return pd.DataFrame(results) if results else pd.DataFrame()


# ── text helpers ──────────────────────────────────────────────────────────────
def clean_description(text: Optional[str]) -> str:
    """Lowercase, strip, return empty string for None."""
    if not text:
        return ""
    return text.strip().lower()
