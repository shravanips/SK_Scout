"""
tests/test_utils.py
-------------------
Unit tests for src/utils.py.

Uses shared fixtures from conftest.py.
GitHub API calls are mocked — no network required.
"""

import logging
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils import (
    clean_description,
    ensure_dir,
    fetch_repo_metadata,
    load_parquet,
    save_parquet,
    setup_logging,
    enrich_repos_with_github_api,
)


# ── setup_logging ─────────────────────────────────────────────────────────────
class TestSetupLogging:
    def test_does_not_raise(self, tmp_path, monkeypatch):
        # Point FileHandler to a writable temp path
        log_path = tmp_path / "logs" / "run.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        monkeypatch.chdir(tmp_path)
        (tmp_path / "logs").mkdir(exist_ok=True)
        # Should not raise
        setup_logging(level=logging.WARNING)

    def test_root_logger_level_set(self, tmp_path, monkeypatch):
        log_path = tmp_path / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        monkeypatch.chdir(tmp_path)
        setup_logging(level=logging.ERROR)
        assert logging.getLogger().level <= logging.ERROR


# ── ensure_dir ────────────────────────────────────────────────────────────────
class TestEnsureDir:
    def test_creates_directory(self, tmp_path):
        new_dir = tmp_path / "a" / "b" / "c"
        result = ensure_dir(new_dir)
        assert new_dir.is_dir()
        assert result == new_dir

    def test_returns_path_object(self, tmp_path):
        result = ensure_dir(tmp_path / "x")
        assert isinstance(result, Path)

    def test_idempotent_on_existing_dir(self, tmp_path):
        ensure_dir(tmp_path)  # already exists — should not raise
        assert tmp_path.is_dir()

    def test_accepts_string(self, tmp_path):
        target = str(tmp_path / "from_string")
        ensure_dir(target)
        assert Path(target).is_dir()


# ── save_parquet / load_parquet ───────────────────────────────────────────────
class TestParquetRoundtrip:
    def test_save_and_load_roundtrip(self, tmp_path, repo_stats_df):
        path = tmp_path / "stats.parquet"
        save_parquet(repo_stats_df, path)
        loaded = load_parquet(path)
        assert len(loaded) == len(repo_stats_df)
        assert list(loaded.columns) == list(repo_stats_df.columns)

    def test_save_creates_parent_dirs(self, tmp_path, repo_stats_df):
        path = tmp_path / "nested" / "deep" / "file.parquet"
        save_parquet(repo_stats_df, path)
        assert path.exists()

    def test_load_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Parquet file not found"):
            load_parquet(tmp_path / "does_not_exist.parquet")

    def test_save_empty_dataframe(self, tmp_path):
        empty = pd.DataFrame({"col": []})
        path = tmp_path / "empty.parquet"
        save_parquet(empty, path)
        loaded = load_parquet(path)
        assert loaded.empty
        assert "col" in loaded.columns

    def test_numeric_columns_preserved(self, tmp_path):
        df = pd.DataFrame({"int_col": [1, 2, 3], "float_col": [1.1, 2.2, 3.3]})
        path = tmp_path / "numeric.parquet"
        save_parquet(df, path)
        loaded = load_parquet(path)
        assert loaded["int_col"].dtype.kind in ("i", "u")
        assert loaded["float_col"].dtype.kind == "f"


# ── clean_description ─────────────────────────────────────────────────────────
class TestCleanDescription:
    def test_lowercases(self):
        assert clean_description("Hello World") == "hello world"

    def test_strips_whitespace(self):
        assert clean_description("  spaces  ") == "spaces"

    def test_none_returns_empty_string(self):
        assert clean_description(None) == ""

    def test_empty_string_returns_empty_string(self):
        assert clean_description("") == ""

    def test_already_clean(self):
        assert clean_description("clean text") == "clean text"


# ── fetch_repo_metadata ───────────────────────────────────────────────────────
class TestFetchRepoMetadata:
    def _mock_response(self, status_code: int, json_data: dict | None = None, headers: dict | None = None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.headers = headers or {}
        resp.raise_for_status = MagicMock()
        return resp

    @patch("utils.requests.get")
    def test_successful_fetch_returns_dict(self, mock_get):
        mock_get.return_value.__enter__ = lambda s: s
        mock_get.return_value.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = self._mock_response(200, {
            "description": "A test repo",
            "language": "Python",
            "topics": ["test"],
            "stargazers_count": 10,
            "forks_count": 2,
            "size": 500,
            "archived": False,
            "created_at": "2024-01-01T00:00:00Z",
            "pushed_at": "2024-06-01T00:00:00Z",
            "default_branch": "main",
            "open_issues_count": 3,
            "license": {"spdx_id": "MIT"},
        })
        result = fetch_repo_metadata("owner/repo")
        assert result is not None
        assert result["language"] == "Python"
        assert result["description"] == "A test repo"

    @patch("utils.requests.get")
    def test_404_returns_none(self, mock_get):
        mock_get.return_value = self._mock_response(404)
        result = fetch_repo_metadata("nonexistent/repo")
        assert result is None

    @patch("utils.requests.get")
    @patch("utils.time.sleep")  # don't actually sleep in tests
    def test_rate_limit_waits_and_retries(self, mock_sleep, mock_get):
        future_reset = int(time.time()) + 2
        rate_limited = self._mock_response(
            403, headers={"X-RateLimit-Reset": str(future_reset)}
        )
        success = self._mock_response(200, {
            "description": "ok", "language": "Go",
            "topics": [], "stargazers_count": 0, "forks_count": 0,
            "size": 0, "archived": False, "created_at": "", "pushed_at": "",
            "default_branch": "main", "open_issues_count": 0, "license": None,
        })
        mock_get.side_effect = [rate_limited, success]
        result = fetch_repo_metadata("owner/repo", retries=2)
        assert mock_sleep.called

    @patch("utils.requests.get")
    @patch("utils.time.sleep")
    def test_network_error_retries(self, mock_sleep, mock_get):
        import requests as req_lib
        mock_get.side_effect = req_lib.RequestException("timeout")
        result = fetch_repo_metadata("owner/repo", retries=2)
        assert result is None
        assert mock_get.call_count == 2


# ── enrich_repos_with_github_api ──────────────────────────────────────────────
class TestEnrichReposWithGithubApi:
    @patch("utils.fetch_repo_metadata")
    def test_returns_dataframe(self, mock_fetch):
        mock_fetch.return_value = {
            "description": "desc", "language": "Python",
            "topics": [], "stargazers_count": 0, "forks_count": 0,
        }
        result = enrich_repos_with_github_api(["owner/repo"], max_repos=1, delay_s=0)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    @patch("utils.fetch_repo_metadata")
    def test_respects_max_repos_cap(self, mock_fetch):
        mock_fetch.return_value = {"description": "x", "language": "Rust", "topics": []}
        repos = [f"owner/repo{i}" for i in range(10)]
        enrich_repos_with_github_api(repos, max_repos=3, delay_s=0)
        assert mock_fetch.call_count == 3

    @patch("utils.fetch_repo_metadata")
    def test_skips_none_results(self, mock_fetch):
        mock_fetch.return_value = None
        result = enrich_repos_with_github_api(["owner/missing"], delay_s=0)
        assert result.empty

    @patch("utils.fetch_repo_metadata")
    def test_empty_input_returns_empty_dataframe(self, mock_fetch):
        result = enrich_repos_with_github_api([], delay_s=0)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        mock_fetch.assert_not_called()
