"""
conftest.py
-----------
Shared pytest fixtures for all test modules.
Reflects the unified ingest output: 4-tuple (events, repos, actors, lockstep).
"""

import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ── raw event data ────────────────────────────────────────────────────────────
SAMPLE_EVENTS = [
    {
        "id": "1", "type": "PushEvent",
        "actor": {"login": "alice", "id": 1},
        "repo": {"name": "alice/my-app", "id": 10},
        "created_at": "2026-04-15T12:00:00Z", "public": True, "org": None,
        "payload": {"commits": [{"message": "fix bug", "author": {"name": "Alice"}}]},
    },
    {
        "id": "2", "type": "PullRequestEvent",
        "actor": {"login": "dependabot[bot]", "id": 2},
        "repo": {"name": "alice/my-app", "id": 10},
        "created_at": "2026-04-15T12:01:00Z", "public": True, "org": None,
        "payload": {},
    },
    {
        "id": "3", "type": "PushEvent",
        "actor": {"login": "github-actions[bot]", "id": 3},
        "repo": {"name": "bob/infra", "id": 11},
        "created_at": "2026-04-15T12:02:00Z", "public": True,
        "org": {"login": "bob-org"}, "payload": {},
    },
    {
        "id": "4", "type": "IssueCommentEvent",
        "actor": {"login": "bob", "id": 4},
        "repo": {"name": "bob/infra", "id": 11},
        "created_at": "2026-04-15T12:03:00Z", "public": True,
        "org": {"login": "bob-org"}, "payload": {},
    },
    {
        "id": "5", "type": "CreateEvent",
        "actor": {"login": "renovate[bot]", "id": 5},
        "repo": {"name": "carol/lib", "id": 12},
        "created_at": "2026-04-15T12:04:00Z", "public": True, "org": None,
        "payload": {"ref": "feature/update-deps"},
    },
    {
        "id": "6", "type": "PushEvent",
        "actor": {"login": "carol", "id": 6},
        "repo": {"name": "carol/lib", "id": 12},
        "created_at": "2026-04-15T12:05:00Z", "public": True, "org": None,
        "payload": {
            "commits": [
                {
                    "message": "Co-authored-by: Claude <claude@anthropic.com>",
                    "author": {"name": "carol"},
                }
            ]
        },
    },
]


def _write_gz(events: list, path: Path) -> Path:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return path


@pytest.fixture(scope="session")
def sample_events() -> list[dict]:
    return SAMPLE_EVENTS


@pytest.fixture
def sample_gz(tmp_path) -> Path:
    return _write_gz(SAMPLE_EVENTS, tmp_path / "sample.json.gz")


@pytest.fixture
def sample_gz_factory(tmp_path):
    def _factory(events: list, name: str = "custom.json.gz") -> Path:
        return _write_gz(events, tmp_path / name)
    return _factory


# ── DataFrame fixtures ────────────────────────────────────────────────────────
@pytest.fixture
def events_df(sample_gz) -> pd.DataFrame:
    from ingest import _parse_file
    rows = list(_parse_file(sample_gz))
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    return df


@pytest.fixture
def repo_stats_df(events_df) -> pd.DataFrame:
    from ingest import compute_repo_stats
    return compute_repo_stats(events_df)


@pytest.fixture
def actor_stats_df(events_df) -> pd.DataFrame:
    from ingest import compute_actor_stats
    return compute_actor_stats(events_df)


@pytest.fixture
def lockstep_df(events_df) -> pd.DataFrame:
    from ingest import detect_lockstep
    return detect_lockstep(events_df, window_minutes=30, min_accounts=2)


@pytest.fixture
def enriched_df() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "repo_name": "alice/my-app",
            "description": "A web application for task management",
            "language": "Python",
            "topics": ["django", "rest-api"],
            "stargazers_count": 120, "forks_count": 15,
            "size_kb": 800, "archived": False,
        },
        {
            "repo_name": "bob/infra",
            "description": "Infrastructure as code with Terraform",
            "language": "HCL",
            "topics": ["terraform", "aws"],
            "stargazers_count": 34, "forks_count": 5,
            "size_kb": 200, "archived": False,
        },
        {
            "repo_name": "carol/lib",
            "description": "Utility library for data processing",
            "language": "Python",
            "topics": ["library", "data"],
            "stargazers_count": 55, "forks_count": 8,
            "size_kb": 150, "archived": False,
        },
    ])


@pytest.fixture
def window_start() -> datetime:
    return datetime(2026, 4, 15, 12, tzinfo=timezone.utc)


@pytest.fixture
def window_end() -> datetime:
    return datetime(2026, 4, 15, 14, tzinfo=timezone.utc)
