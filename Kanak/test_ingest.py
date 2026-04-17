"""
tests/test_ingest.py
--------------------
Unit tests for src/ingest.py — no network calls required.
"""

import gzip
import json
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

# allow importing from src/
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingest import _is_bot, _parse_file, compute_repo_stats


# ── fixtures ──────────────────────────────────────────────────────────────────
def _make_gz(events: list, path: Path) -> Path:
    """Write a list of event dicts as a .json.gz file."""
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return path


SAMPLE_EVENTS = [
    {
        "id": "1",
        "type": "PushEvent",
        "actor": {"login": "alice", "id": 1},
        "repo":  {"name": "alice/my-app", "id": 10},
        "created_at": "2026-04-15T12:00:00Z",
        "public": True,
    },
    {
        "id": "2",
        "type": "PullRequestEvent",
        "actor": {"login": "dependabot[bot]", "id": 2},
        "repo":  {"name": "alice/my-app", "id": 10},
        "created_at": "2026-04-15T12:01:00Z",
        "public": True,
    },
    {
        "id": "3",
        "type": "PushEvent",
        "actor": {"login": "github-actions[bot]", "id": 3},
        "repo":  {"name": "bob/infra", "id": 11},
        "created_at": "2026-04-15T12:02:00Z",
        "public": True,
    },
    {
        "id": "4",
        "type": "IssueCommentEvent",
        "actor": {"login": "bob", "id": 4},
        "repo":  {"name": "bob/infra", "id": 11},
        "created_at": "2026-04-15T12:03:00Z",
        "public": True,
    },
]


# ── _is_bot tests ─────────────────────────────────────────────────────────────
class TestIsBot:
    def test_bracket_bot_suffix(self):
        assert _is_bot("dependabot[bot]") is True

    def test_known_bot_name(self):
        assert _is_bot("renovate") is True

    def test_github_actions(self):
        assert _is_bot("github-actions[bot]") is True

    def test_human_user(self):
        assert _is_bot("alice") is False

    def test_none(self):
        assert _is_bot(None) is False

    def test_empty(self):
        assert _is_bot("") is False


# ── _parse_file tests ─────────────────────────────────────────────────────────
class TestParseFile:
    def test_basic_parsing(self, tmp_path):
        gz = _make_gz(SAMPLE_EVENTS, tmp_path / "test.json.gz")
        rows = list(_parse_file(gz))
        assert len(rows) == 4

    def test_fields_present(self, tmp_path):
        gz = _make_gz(SAMPLE_EVENTS[:1], tmp_path / "test.json.gz")
        row = list(_parse_file(gz))[0]
        required = {"event_id", "event_type", "actor_login", "repo_name",
                    "created_at", "is_bot_actor"}
        assert required.issubset(set(row.keys()))

    def test_bot_flag(self, tmp_path):
        gz = _make_gz(SAMPLE_EVENTS, tmp_path / "test.json.gz")
        rows = list(_parse_file(gz))
        df = pd.DataFrame(rows)
        bots = df[df["is_bot_actor"]]
        assert set(bots["actor_login"]) == {"dependabot[bot]", "github-actions[bot]"}

    def test_max_events_cap(self, tmp_path):
        gz = _make_gz(SAMPLE_EVENTS, tmp_path / "test.json.gz")
        rows = list(_parse_file(gz, max_events=2))
        assert len(rows) == 2

    def test_bad_json_skipped(self, tmp_path):
        gz_path = tmp_path / "bad.json.gz"
        with gzip.open(gz_path, "wt") as f:
            f.write(json.dumps(SAMPLE_EVENTS[0]) + "\n")
            f.write("NOT JSON\n")
            f.write(json.dumps(SAMPLE_EVENTS[1]) + "\n")
        rows = list(_parse_file(gz_path))
        assert len(rows) == 2   # bad line skipped


# ── compute_repo_stats tests ──────────────────────────────────────────────────
class TestComputeRepoStats:
    @pytest.fixture
    def sample_df(self, tmp_path):
        gz = _make_gz(SAMPLE_EVENTS, tmp_path / "test.json.gz")
        rows = list(_parse_file(gz))
        df = pd.DataFrame(rows)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df

    def test_returns_dataframe(self, sample_df):
        stats = compute_repo_stats(sample_df)
        assert isinstance(stats, pd.DataFrame)

    def test_repo_count(self, sample_df):
        stats = compute_repo_stats(sample_df)
        assert len(stats) == 2   # alice/my-app and bob/infra

    def test_bot_ratio_range(self, sample_df):
        stats = compute_repo_stats(sample_df)
        assert stats["bot_ratio"].between(0, 1).all()

    def test_alice_repo_bot_ratio(self, sample_df):
        stats = compute_repo_stats(sample_df)
        row = stats[stats["repo_name"] == "alice/my-app"].iloc[0]
        # 1 bot event out of 2 total → 0.5
        assert row["bot_ratio"] == pytest.approx(0.5)

    def test_suspicious_score_non_negative(self, sample_df):
        stats = compute_repo_stats(sample_df)
        assert (stats["suspicious_score"] >= 0).all()
