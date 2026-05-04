"""
tests/test_analytics.py
-----------------------
Unit tests for src/analytics.py — all 8 classes.

Coverage by origin
------------------
Kanak classes:    BotActorProfiler, RepoPurposeAnalyser, BotRepoCorrelation,
                  AnomalyDetector (extended), ReportExporter
Shravani classes: SuspiciousHumanAnalyser, LockstepAnalyser,
                  PhishingRepoAnalyser
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analytics import (
    BotActorProfiler,
    SuspiciousHumanAnalyser,
    RepoPurposeAnalyser,
    BotRepoCorrelation,
    LockstepAnalyser,
    PhishingRepoAnalyser,
    AnomalyDetector,
    ReportExporter,
    classify_bot,
)


# ── classify_bot ──────────────────────────────────────────────────────────────
class TestClassifyBot:
    def test_dependency(self):    assert classify_bot("dependabot[bot]") == "dependency"
    def test_renovate(self):      assert classify_bot("renovate") == "dependency"
    def test_cicd(self):          assert classify_bot("github-actions[bot]") == "ci_cd"
    def test_security(self):      assert classify_bot("snyk-bot") == "security"
    def test_code_quality(self):  assert classify_bot("codecov") == "code_quality"
    def test_docs(self):          assert classify_bot("allcontributors[bot]") == "docs"
    def test_translation(self):   assert classify_bot("crowdin-bot") == "translation"
    def test_other(self):         assert classify_bot("mystery-bot") == "other"
    def test_case_insensitive(self): assert classify_bot("DEPENDABOT[BOT]") == "dependency"


# ── BotActorProfiler (Kanak) ──────────────────────────────────────────────────
class TestBotActorProfiler:
    @pytest.fixture
    def profiler(self, events_df):
        return BotActorProfiler(events_df)

    @pytest.fixture
    def no_bot_profiler(self, events_df):
        return BotActorProfiler(events_df[~events_df["is_bot_actor"]].copy())

    def test_returns_dataframe(self, profiler):
        assert isinstance(profiler.profile(), pd.DataFrame)

    def test_only_bots_in_profile(self, profiler, events_df):
        bot_logins = set(events_df[events_df["is_bot_actor"]]["actor_login"].unique())
        assert set(profiler.profile()["actor_login"]).issubset(bot_logins)

    def test_required_columns(self, profiler):
        required = {"actor_login", "total_events", "unique_repos",
                    "bot_category", "wide_footprint", "events_per_hour"}
        assert required.issubset(profiler.profile().columns)

    def test_empty_on_no_bots(self, no_bot_profiler):
        assert no_bot_profiler.profile().empty

    def test_category_assigned(self, profiler):
        assert profiler.profile()["bot_category"].notna().all()

    def test_wide_footprint_bool(self, profiler):
        assert profiler.profile()["wide_footprint"].dtype == bool

    def test_events_per_hour_non_negative(self, profiler):
        assert (profiler.profile()["events_per_hour"] >= 0).all()

    def test_category_summary_returns_df(self, profiler):
        assert isinstance(profiler.category_summary(), pd.DataFrame)

    def test_detect_bursts_high_threshold_empty(self, profiler):
        assert profiler.detect_bursts(threshold=100_000).empty

    def test_detect_bursts_low_threshold_has_rows(self, profiler):
        result = profiler.detect_bursts(threshold=1)
        assert not result.empty

    def test_event_type_breakdown_returns_df(self, profiler):
        assert isinstance(profiler.event_type_breakdown(), pd.DataFrame)


# ── SuspiciousHumanAnalyser (Shravani) ────────────────────────────────────────
class TestSuspiciousHumanAnalyser:
    @pytest.fixture
    def analyser(self, actor_stats_df):
        return SuspiciousHumanAnalyser(actor_stats_df)

    def test_returns_dataframe(self, analyser):
        assert isinstance(analyser.top_suspicious(), pd.DataFrame)

    def test_excludes_known_bots(self, analyser, actor_stats_df):
        result = analyser.top_suspicious()
        if not result.empty:
            assert not result["is_bot_actor"].any()

    def test_min_score_respected(self, analyser):
        result = analyser.top_suspicious(min_score=10)
        if not result.empty:
            assert (result["suspicious_human_score"] >= 10).all()

    def test_top_n_respected(self, analyser):
        result = analyser.top_suspicious(min_score=0, top_n=2)
        assert len(result) <= 2

    def test_score_distribution_returns_series(self, analyser):
        assert isinstance(analyser.score_distribution(), pd.Series)

    def test_ai_coauthor_accounts_returns_df(self, analyser):
        assert isinstance(analyser.ai_coauthor_accounts(), pd.DataFrame)

    def test_ai_coauthor_flag_true(self, analyser):
        # carol has an AI co-author in conftest sample data
        result = analyser.ai_coauthor_accounts()
        if not result.empty:
            assert result["ai_coauthor"].all()

    def test_empty_input_returns_empty(self):
        empty_stats = pd.DataFrame(columns=["is_bot_actor", "suspicious_human_score"])
        analyser = SuspiciousHumanAnalyser(empty_stats)
        assert analyser.top_suspicious().empty


# ── LockstepAnalyser (Shravani) ───────────────────────────────────────────────
class TestLockstepAnalyser:
    @pytest.fixture
    def analyser(self, lockstep_df):
        return LockstepAnalyser(lockstep_df)

    @pytest.fixture
    def empty_analyser(self):
        return LockstepAnalyser(pd.DataFrame())

    def test_top_targeted_returns_df(self, analyser):
        assert isinstance(analyser.top_targeted_repos(), pd.DataFrame)

    def test_empty_input_returns_empty(self, empty_analyser):
        assert empty_analyser.top_targeted_repos().empty
        assert empty_analyser.repeated_actor_clusters().empty

    def test_top_n_respected(self, analyser):
        result = analyser.top_targeted_repos(top_n=1)
        assert len(result) <= 1

    def test_repeated_actor_clusters_returns_df(self, analyser):
        assert isinstance(analyser.repeated_actor_clusters(), pd.DataFrame)

    def test_top_targeted_columns(self, analyser):
        result = analyser.top_targeted_repos()
        if not result.empty:
            assert {"repo_name", "lockstep_windows",
                    "max_actors", "total_events"}.issubset(result.columns)


# ── RepoPurposeAnalyser (Kanak + Shravani suspicious category) ────────────────
class TestRepoPurposeAnalyser:
    @pytest.fixture
    def analyser(self, repo_stats_df, enriched_df):
        return RepoPurposeAnalyser(repo_stats_df, enriched_df=enriched_df, n_clusters=2)

    @pytest.fixture
    def analyser_no_enrich(self, repo_stats_df):
        return RepoPurposeAnalyser(repo_stats_df, n_clusters=2)

    def test_language_distribution(self, analyser):
        result = analyser.language_distribution()
        assert "Python" in result.index

    def test_cluster_adds_columns(self, analyser):
        df = analyser.cluster_by_description()
        assert "purpose_cluster" in df.columns and "purpose_label" in df.columns

    def test_cluster_graceful_without_enrichment(self, analyser_no_enrich):
        df = analyser_no_enrich.cluster_by_description()
        assert "purpose_cluster" in df.columns

    def test_name_heuristic_adds_column(self, analyser_no_enrich):
        df = analyser_no_enrich.name_heuristic_classify()
        assert "name_category" in df.columns

    def test_suspicious_category_available(self, repo_stats_df):
        # Inject a suspicious-name repo
        suspicious_row = repo_stats_df.iloc[:1].copy()
        suspicious_row["repo_name"] = "owner/roblox-hack"
        df = pd.concat([repo_stats_df, suspicious_row], ignore_index=True)
        analyser = RepoPurposeAnalyser(df, n_clusters=2)
        result = analyser.name_heuristic_classify()
        assert "suspicious" in result["name_category"].values

    def test_bot_heavy_returns_df(self, analyser, events_df):
        assert isinstance(analyser.bot_heavy_repo_purposes(events_df), pd.DataFrame)


# ── BotRepoCorrelation (Kanak) ────────────────────────────────────────────────
class TestBotRepoCorrelation:
    @pytest.fixture
    def correlation(self, events_df, repo_stats_df, enriched_df):
        analyser = RepoPurposeAnalyser(repo_stats_df, enriched_df=enriched_df, n_clusters=2)
        analyser.cluster_by_description()
        analyser.name_heuristic_classify()
        return BotRepoCorrelation(events_df, analyser._get_merged())

    def test_xtab_returns_df(self, correlation):
        assert isinstance(correlation.bot_category_x_purpose(), pd.DataFrame)

    def test_xtab_has_all_margin(self, correlation):
        xtab = correlation.bot_category_x_purpose()
        assert "All" in xtab.index or "All" in xtab.columns

    def test_top_repos_returns_df(self, correlation):
        assert isinstance(correlation.top_repos_per_bot(), pd.DataFrame)

    def test_top_repos_rank_within_bounds(self, correlation):
        result = correlation.top_repos_per_bot(top_n=3)
        if not result.empty:
            assert (result["rank"] <= 3).all()


# ── PhishingRepoAnalyser (Shravani) ───────────────────────────────────────────
class TestPhishingRepoAnalyser:
    @pytest.fixture
    def analyser(self, repo_stats_df):
        return PhishingRepoAnalyser(repo_stats_df)

    def test_high_risk_returns_df(self, analyser):
        assert isinstance(analyser.high_risk_repos(), pd.DataFrame)

    def test_high_risk_all_above_threshold(self, analyser):
        result = analyser.high_risk_repos(min_score=1)
        if not result.empty:
            assert (result["suspicious_score"] >= 1).all()

    def test_phish_name_repos_returns_df(self, analyser):
        assert isinstance(analyser.phish_name_repos(), pd.DataFrame)

    def test_branch_explosion_returns_df(self, analyser):
        assert isinstance(analyser.branch_explosion_repos(), pd.DataFrame)

    def test_ai_coauthor_repos_returns_df(self, analyser):
        assert isinstance(analyser.ai_coauthor_repos(), pd.DataFrame)

    def test_risk_breakdown_returns_df(self, analyser):
        rb = analyser.risk_breakdown()
        assert isinstance(rb, pd.DataFrame)
        if not rb.empty:
            assert {"signal", "repo_count"}.issubset(rb.columns)

    def test_missing_columns_handled(self):
        analyser = PhishingRepoAnalyser(pd.DataFrame({"repo_name": ["x/y"]}))
        assert analyser.high_risk_repos().empty
        assert analyser.phish_name_repos().empty
        assert analyser.branch_explosion_repos().empty


# ── AnomalyDetector (Kanak + Shravani added distinct_branches) ───────────────
class TestAnomalyDetector:
    def test_returns_df(self, repo_stats_df):
        assert isinstance(AnomalyDetector(repo_stats_df).zscore_anomalies(), pd.DataFrame)

    def test_low_threshold_catches_rows(self, repo_stats_df):
        assert not AnomalyDetector(repo_stats_df, z_threshold=0.0).zscore_anomalies().empty

    def test_high_threshold_empty(self, repo_stats_df):
        assert AnomalyDetector(repo_stats_df, z_threshold=999.0).zscore_anomalies().empty

    def test_columns_present(self, repo_stats_df):
        result = AnomalyDetector(repo_stats_df, z_threshold=0.0).zscore_anomalies()
        assert "max_z" in result.columns and "anomaly_feature" in result.columns

    def test_distinct_branches_in_features(self, repo_stats_df):
        # distinct_branches added by Shravani should be included when present
        if "distinct_branches" in repo_stats_df.columns:
            features = AnomalyDetector.FEATURE_COLS
            assert "distinct_branches" in features

    def test_sorted_descending(self, repo_stats_df):
        result = AnomalyDetector(repo_stats_df, z_threshold=0.0).zscore_anomalies()
        if len(result) > 1:
            assert result["max_z"].is_monotonic_decreasing

    def test_empty_stats_returns_empty(self):
        assert AnomalyDetector(pd.DataFrame({"repo_name": ["x"]})).zscore_anomalies().empty


# ── ReportExporter ────────────────────────────────────────────────────────────
class TestReportExporter:
    def test_save_csv_creates_file(self, tmp_path, repo_stats_df):
        path = ReportExporter(output_dir=tmp_path).save_csv(repo_stats_df, "test")
        assert path is not None and path.exists()

    def test_save_csv_skips_empty(self, tmp_path):
        result = ReportExporter(output_dir=tmp_path).save_csv(pd.DataFrame(), "empty")
        assert result is None

    def test_save_html_creates_file(self, tmp_path, events_df, repo_stats_df):
        exporter = ReportExporter(output_dir=tmp_path)
        profiler = BotActorProfiler(events_df)
        path = exporter.save_html_report(
            bot_profile       = profiler.profile(),
            suspicious_humans = pd.DataFrame(),
            lockstep_repos    = pd.DataFrame(),
            phish_repos       = pd.DataFrame(),
            risk_breakdown    = pd.DataFrame(),
            anomalies         = pd.DataFrame(),
            bot_cat_summary   = profiler.category_summary(),
            branch_explosion  = pd.DataFrame(),
            ai_coauthor_repos = pd.DataFrame(),
            correlation_xtab  = pd.DataFrame(),
            total_repos       = len(repo_stats_df),
        )
        assert path.exists()
        assert "<html" in path.read_text(encoding="utf-8").lower()

    def test_save_html_all_empty_inputs(self, tmp_path):
        exporter = ReportExporter(output_dir=tmp_path)
        path = exporter.save_html_report(
            bot_profile=pd.DataFrame(), suspicious_humans=pd.DataFrame(),
            lockstep_repos=pd.DataFrame(), phish_repos=pd.DataFrame(),
            risk_breakdown=pd.DataFrame(), anomalies=pd.DataFrame(),
            bot_cat_summary=pd.DataFrame(), branch_explosion=pd.DataFrame(),
            ai_coauthor_repos=pd.DataFrame(), correlation_xtab=pd.DataFrame(),
            total_repos=0,
        )
        assert path.exists()
