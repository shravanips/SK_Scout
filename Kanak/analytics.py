"""
analytics.py
------------
Deep analysis of bot actors and the repositories they operate on.

Modules
-------
1. BotActorProfiler    – Who are the bots? Behaviour + volume analysis.
2. RepoPurposeAnalyser – What are the repos about?
                         (keyword clustering, language distribution, topic tags,
                          README-based classification via GitHub API)
3. BotRepoCorrelation  – Which bot types gravitate to which repo types?
4. AnomalyDetector     – Statistical outliers in bot activity.
5. ReportExporter      – Saves HTML / CSV / JSON reports to data/reports/.

Unique additions beyond the original notebook
---------------------------------------------
* GitHub API enrichment: fetches description, language, topics for each repo.
* TF-IDF + KMeans clustering of repo descriptions → inferred purpose clusters.
* Bot taxonomy: classifies bots into functional categories
  (dependency, CI, security, translation, docs, other).
* Temporal burst detection: flags repos where bot activity spikes
  within a narrow time window.
* Cross-repo bot footprint: identifies bots active across many repos
  (coordination signal).
* Interactive HTML report with embedded Plotly charts.
"""

import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler

from utils import (
    ensure_dir,
    load_parquet,
    save_parquet,
    enrich_repos_with_github_api,
    clean_description,
    setup_logging,
)

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
REPORTS_DIR   = Path("data/reports")

# ── bot taxonomy ──────────────────────────────────────────────────────────────
BOT_TAXONOMY: dict[str, list[str]] = {
    "dependency":  ["dependabot", "renovate", "greenkeeper", "depfu"],
    "ci_cd":       ["github-actions", "travis", "circleci", "semantic-release"],
    "security":    ["snyk-bot", "mend-bolt", "whitesource"],
    "code_quality":["codecov", "codeclimate", "deepsource"],
    "docs":        ["allcontributors", "imgbot", "readme-bot"],
    "translation": ["lokalise", "crowdin", "transifex"],
}

def classify_bot(login: str) -> str:
    """Return a functional category for a bot login."""
    login_lower = login.lower()
    for category, keywords in BOT_TAXONOMY.items():
        if any(kw in login_lower for kw in keywords):
            return category
    return "other"


# ── 1. BotActorProfiler ───────────────────────────────────────────────────────
class BotActorProfiler:
    """
    Analyse the behaviour of bot actors in the event stream.
    """

    def __init__(self, events_df: pd.DataFrame):
        self.df = events_df[events_df["is_bot_actor"]].copy()
        if self.df.empty:
            logger.warning("No bot events found in the dataset.")

    def profile(self) -> pd.DataFrame:
        """
        Per-bot statistics:
          - event volume and diversity
          - repos touched
          - functional category
          - activity time range
          - cross-repo footprint (# distinct repos)
        """
        if self.df.empty:
            return pd.DataFrame()

        profile = self.df.groupby("actor_login", sort=False).agg(
            total_events        =("event_id",    "count"),
            unique_repos        =("repo_name",   "nunique"),
            event_type_diversity=("event_type",  "nunique"),
            first_seen          =("created_at",  "min"),
            last_seen           =("created_at",  "max"),
        ).reset_index()

        profile["activity_span_h"] = (
            (profile["last_seen"] - profile["first_seen"])
            .dt.total_seconds() / 3600
        )
        profile["events_per_hour"] = (
            profile["total_events"] /
            profile["activity_span_h"].replace(0, 1)
        )
        profile["bot_category"] = profile["actor_login"].apply(classify_bot)

        # cross-repo footprint flag: bot active in 5+ repos
        profile["wide_footprint"] = profile["unique_repos"] >= 5

        return profile.sort_values("total_events", ascending=False)

    def event_type_breakdown(self) -> pd.DataFrame:
        """Cross-tab: bot_login × event_type counts."""
        if self.df.empty:
            return pd.DataFrame()
        return pd.crosstab(
            self.df["actor_login"],
            self.df["event_type"],
        )

    def category_summary(self) -> pd.DataFrame:
        """Aggregate bot profile by functional category."""
        profile = self.profile()
        if profile.empty:
            return pd.DataFrame()
        return profile.groupby("bot_category").agg(
            n_bots            =("actor_login",    "count"),
            total_events      =("total_events",   "sum"),
            median_repos      =("unique_repos",   "median"),
            wide_footprint_pct=("wide_footprint", "mean"),
        ).reset_index()

    def detect_bursts(self, window_minutes: int = 10, threshold: int = 20) -> pd.DataFrame:
        """
        Find (bot, time_window) pairs where event count exceeds `threshold`.
        Useful for identifying automation that fires in tight bursts.
        """
        if self.df.empty:
            return pd.DataFrame()
        tmp = self.df.copy()
        tmp = tmp.set_index("created_at").sort_index()
        results = []
        for bot, grp in tmp.groupby("actor_login"):
            counts = grp["event_id"].resample(f"{window_minutes}min").count()
            bursts = counts[counts >= threshold]
            for ts, cnt in bursts.items():
                results.append({
                    "actor_login": bot,
                    "window_start": ts,
                    "event_count": cnt,
                })
        return pd.DataFrame(results)


# ── 2. RepoPurposeAnalyser ────────────────────────────────────────────────────
class RepoPurposeAnalyser:
    """
    Classify / cluster repos by their purpose using:
      - GitHub API metadata (language, topics, description)
      - TF-IDF + KMeans unsupervised clustering
      - Rule-based heuristics from repo name patterns
    """

    def __init__(
        self,
        repo_stats_df: pd.DataFrame,
        enriched_df: Optional[pd.DataFrame] = None,
        n_clusters: int = 8,
    ):
        self.stats = repo_stats_df.copy()
        self.enriched = enriched_df  # from GitHub API; may be None
        self.n_clusters = n_clusters
        self._merged: Optional[pd.DataFrame] = None

    def _get_merged(self) -> pd.DataFrame:
        if self._merged is not None:
            return self._merged
        if self.enriched is not None and not self.enriched.empty:
            self._merged = self.stats.merge(
                self.enriched, on="repo_name", how="left"
            )
        else:
            self._merged = self.stats.copy()
        return self._merged

    def language_distribution(self) -> pd.Series:
        df = self._get_merged()
        if "language" not in df.columns:
            return pd.Series(dtype=int)
        return df["language"].fillna("Unknown").value_counts()

    def topic_frequency(self) -> pd.Series:
        df = self._get_merged()
        if "topics" not in df.columns:
            return pd.Series(dtype=int)
        from collections import Counter
        counter: Counter = Counter()
        for topics in df["topics"].dropna():
            counter.update(topics)
        return pd.Series(counter).sort_values(ascending=False)

    def cluster_by_description(self) -> pd.DataFrame:
        """
        Cluster repos by TF-IDF of their description text.
        Returns the merged DataFrame with a 'purpose_cluster' column added.
        """
        df = self._get_merged().copy()
        if "description" not in df.columns or df["description"].isna().all():
            logger.warning("No description data available; skipping clustering.")
            df["purpose_cluster"] = -1
            df["purpose_label"] = "unknown"
            return df

        texts = df["description"].fillna("").apply(clean_description)
        valid_mask = texts.str.len() > 5

        if valid_mask.sum() < self.n_clusters:
            logger.warning(
                "Too few valid descriptions (%d) for %d clusters.",
                valid_mask.sum(), self.n_clusters,
            )
            df["purpose_cluster"] = -1
            df["purpose_label"] = "unknown"
            return df

        vec = TfidfVectorizer(
            max_features=500,
            stop_words="english",
            ngram_range=(1, 2),
            min_df=2,
        )
        X = vec.fit_transform(texts[valid_mask])

        km = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        labels = km.fit_predict(X)

        df.loc[valid_mask, "purpose_cluster"] = labels.astype(int)
        df["purpose_cluster"] = df["purpose_cluster"].fillna(-1).astype(int)

        # label each cluster by its top-3 TF-IDF terms
        terms = vec.get_feature_names_out()
        order = km.cluster_centers_.argsort()[:, ::-1]
        cluster_labels = {
            i: "_".join(terms[order[i, :3]])
            for i in range(self.n_clusters)
        }
        cluster_labels[-1] = "no_description"
        df["purpose_label"] = df["purpose_cluster"].map(cluster_labels)

        self._merged = df
        return df

    def name_heuristic_classify(self) -> pd.DataFrame:
        """
        Simple rule-based classifier from the repo name itself.
        Returns a column 'name_category'.
        """
        df = self._get_merged().copy()
        name = df["repo_name"].str.lower()

        conditions = [
            name.str.contains(r"config|dotfile|setting|rc$", regex=True),
            name.str.contains(r"bot|automation|action|workflow", regex=True),
            name.str.contains(r"demo|example|sample|test|template", regex=True),
            name.str.contains(r"docs|documentation|wiki", regex=True),
            name.str.contains(r"awesome-|list|collection|resource", regex=True),
        ]
        choices = ["config", "automation", "demo_or_test", "docs", "curated_list"]
        df["name_category"] = np.select(conditions, choices, default="general")
        self._merged = df
        return df

    def bot_heavy_repo_purposes(
        self, events_df: pd.DataFrame, bot_ratio_threshold: float = 0.5
    ) -> pd.DataFrame:
        """
        Subset repos where >50% of events come from bots, then describe
        their inferred purposes. This is the core 'what are bot repos about'
        question.
        """
        df = self._get_merged()
        if "bot_ratio" not in df.columns:
            logger.warning("bot_ratio column missing; run ingest first.")
            return pd.DataFrame()
        bot_heavy = df[df["bot_ratio"] >= bot_ratio_threshold].copy()
        logger.info("%d bot-heavy repos (bot_ratio >= %.0f%%)",
                    len(bot_heavy), bot_ratio_threshold * 100)
        return bot_heavy


# ── 3. BotRepoCorrelation ─────────────────────────────────────────────────────
class BotRepoCorrelation:
    """
    Link bot categories to repo purpose clusters, revealing which
    bot types tend to work in which kinds of project.
    """

    def __init__(
        self,
        events_df: pd.DataFrame,
        repo_purpose_df: pd.DataFrame,   # output of RepoPurposeAnalyser
    ):
        self.events = events_df.copy()
        self.repo_meta = repo_purpose_df[
            ["repo_name"] +
            [c for c in ["purpose_label", "name_category", "language"]
             if c in repo_purpose_df.columns]
        ].copy()

    def bot_category_x_purpose(self) -> pd.DataFrame:
        """Cross-tab: bot category × repo purpose cluster."""
        bots = self.events[self.events["is_bot_actor"]].copy()
        bots["bot_category"] = bots["actor_login"].apply(classify_bot)
        merged = bots.merge(self.repo_meta, on="repo_name", how="left")

        if "purpose_label" not in merged.columns:
            merged["purpose_label"] = "unknown"

        return pd.crosstab(
            merged["bot_category"],
            merged["purpose_label"],
            margins=True,
        )

    def top_repos_per_bot(self, top_n: int = 5) -> pd.DataFrame:
        """For each bot actor, list its top-N repos by event count."""
        bots = self.events[self.events["is_bot_actor"]]
        counts = (
            bots.groupby(["actor_login", "repo_name"])
            .size()
            .reset_index(name="event_count")
        )
        counts["rank"] = counts.groupby("actor_login")["event_count"].rank(
            ascending=False, method="first"
        )
        return counts[counts["rank"] <= top_n].merge(
            self.repo_meta, on="repo_name", how="left"
        ).sort_values(["actor_login", "rank"])


# ── 4. AnomalyDetector ───────────────────────────────────────────────────────
class AnomalyDetector:
    """
    Detect statistical anomalies in bot-repo interaction patterns
    using Z-score thresholding on key numeric signals.
    """

    FEATURE_COLS = [
        "total_events", "unique_actors", "bot_ratio",
        "events_per_actor", "events_per_second",
    ]

    def __init__(self, repo_stats_df: pd.DataFrame, z_threshold: float = 3.0):
        self.stats = repo_stats_df.copy()
        self.z_threshold = z_threshold

    def zscore_anomalies(self) -> pd.DataFrame:
        """
        Return repos that are statistical outliers (|z| > threshold)
        on any numeric feature column.
        """
        available = [c for c in self.FEATURE_COLS if c in self.stats.columns]
        if not available:
            return pd.DataFrame()

        scaler = StandardScaler()
        numeric = self.stats[available].fillna(0)
        z_scores = pd.DataFrame(
            scaler.fit_transform(numeric),
            columns=[f"z_{c}" for c in available],
            index=self.stats.index,
        )
        combined = pd.concat([self.stats[["repo_name"] + available], z_scores], axis=1)
        combined["max_z"] = z_scores.abs().max(axis=1)
        combined["anomaly_feature"] = z_scores.abs().idxmax(axis=1).str.replace("z_", "")
        return (
            combined[combined["max_z"] > self.z_threshold]
            .sort_values("max_z", ascending=False)
        )


# ── 5. ReportExporter ────────────────────────────────────────────────────────
class ReportExporter:
    """
    Save analysis results to data/reports/ in CSV and a self-contained
    HTML summary with embedded Plotly charts.
    """

    def __init__(self, output_dir: Path = REPORTS_DIR):
        self.out = ensure_dir(output_dir)

    def save_csv(self, df: pd.DataFrame, name: str) -> Path:
        path = self.out / f"{name}.csv"
        df.to_csv(path, index=False)
        logger.info("CSV saved: %s", path)
        return path

    def save_html_report(
        self,
        bot_profile: pd.DataFrame,
        repo_purposes: pd.DataFrame,
        correlation_xtab: pd.DataFrame,
        anomalies: pd.DataFrame,
    ) -> Path:
        """Generate a self-contained HTML report with Plotly charts."""
        try:
            import plotly.express as px
            import plotly.io as pio
            _HAS_PLOTLY = True
        except ImportError:
            _HAS_PLOTLY = False
            logger.warning("plotly not installed; HTML report will be text-only.")

        sections: list[str] = []

        # ── header ────────────────────────────────────────────────────────────
        sections.append("""
<html><head>
<meta charset='utf-8'>
<title>GitGub Bot Analysis Report</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 1100px;
         margin: 40px auto; color: #222; }
  h1   { color: #1a1a2e; }
  h2   { color: #16213e; border-bottom: 2px solid #e94560; padding-bottom:4px; }
  table { border-collapse: collapse; width:100%; font-size:.9em; }
  th, td { border:1px solid #ddd; padding:6px 10px; }
  th { background:#16213e; color:#fff; }
  tr:nth-child(even) { background:#f4f4f4; }
</style>
</head><body>
<h1>🤖 GitGub Bot Activity Report</h1>
""")

        def _df_to_html(df: pd.DataFrame, head: int = 20) -> str:
            return df.head(head).to_html(index=False, border=0, classes="data-table")

        # ── bot profile ───────────────────────────────────────────────────────
        sections.append("<h2>1 · Bot Actor Profiles</h2>")
        if not bot_profile.empty:
            sections.append(_df_to_html(bot_profile))
            if _HAS_PLOTLY:
                if "bot_category" in bot_profile.columns:
                    fig = px.bar(
                        bot_profile.groupby("bot_category")["total_events"].sum()
                                   .reset_index(),
                        x="bot_category", y="total_events",
                        title="Total Events by Bot Category",
                        color="bot_category",
                    )
                    sections.append(pio.to_html(fig, full_html=False))

        # ── repo purposes ─────────────────────────────────────────────────────
        sections.append("<h2>2 · Bot-Heavy Repo Purposes</h2>")
        if not repo_purposes.empty:
            cols = [c for c in ["repo_name", "purpose_label", "name_category",
                                "language", "bot_ratio", "total_events",
                                "suspicious_score"]
                    if c in repo_purposes.columns]
            sections.append(_df_to_html(repo_purposes[cols]))
            if _HAS_PLOTLY and "language" in repo_purposes.columns:
                lang_counts = (
                    repo_purposes["language"].fillna("Unknown").value_counts()
                                             .head(10).reset_index()
                )
                lang_counts.columns = ["language", "count"]
                fig2 = px.pie(lang_counts, names="language", values="count",
                              title="Language Distribution of Bot-Heavy Repos")
                sections.append(pio.to_html(fig2, full_html=False))

        # ── correlation ───────────────────────────────────────────────────────
        sections.append("<h2>3 · Bot Category × Repo Purpose</h2>")
        if not correlation_xtab.empty:
            sections.append(correlation_xtab.to_html(border=0, classes="data-table"))

        # ── anomalies ─────────────────────────────────────────────────────────
        sections.append("<h2>4 · Statistical Anomalies</h2>")
        if not anomalies.empty:
            sections.append(_df_to_html(anomalies))

        sections.append("</body></html>")

        html = "\n".join(sections)
        path = self.out / "report.html"
        path.write_text(html, encoding="utf-8")
        logger.info("HTML report saved: %s", path)
        return path


# ── orchestration ─────────────────────────────────────────────────────────────
def run_analytics(
    events_prefix: str = "events",
    enrich_with_github: bool = True,
    max_enrich_repos: int = 300,
    n_clusters: int = 8,
) -> None:
    """
    Load processed Parquet files and run the full analytics pipeline.

    Parameters
    ----------
    events_prefix : str
        Prefix used when saving files in ingest.py (default 'events').
    enrich_with_github : bool
        Whether to call the GitHub API for repo metadata.
        Requires GITHUB_TOKEN env var for high rate limits.
    max_enrich_repos : int
        Cap on how many repos to enrich via the API.
    n_clusters : int
        Number of KMeans clusters for repo purpose analysis.
    """
    setup_logging()
    ensure_dir(REPORTS_DIR)

    # ── load data ─────────────────────────────────────────────────────────────
    events_path = PROCESSED_DIR / f"{events_prefix}_raw.parquet"
    stats_path  = PROCESSED_DIR / f"{events_prefix}_repo_stats.parquet"

    logger.info("Loading events from %s", events_path)
    df_events = load_parquet(events_path)

    logger.info("Loading repo stats from %s", stats_path)
    df_stats  = load_parquet(stats_path)

    # ── optional GitHub API enrichment ────────────────────────────────────────
    enriched_df = None
    enriched_path = PROCESSED_DIR / "repo_metadata.parquet"

    if enrich_with_github:
        if enriched_path.exists():
            logger.info("Loading cached repo metadata from %s", enriched_path)
            enriched_df = load_parquet(enriched_path)
        else:
            top_repos = df_stats["repo_name"].head(max_enrich_repos).tolist()
            logger.info("Fetching GitHub API metadata for %d repos …", len(top_repos))
            enriched_df = enrich_repos_with_github_api(top_repos, max_repos=max_enrich_repos)
            if not enriched_df.empty:
                save_parquet(enriched_df, enriched_path)

    # ── 1. Bot actor profiling ────────────────────────────────────────────────
    logger.info("=== Bot Actor Profiling ===")
    profiler = BotActorProfiler(df_events)
    bot_profile   = profiler.profile()
    bot_bursts    = profiler.detect_bursts()
    bot_cat_summ  = profiler.category_summary()

    logger.info("Bot actors found: %d", len(bot_profile))
    if not bot_bursts.empty:
        logger.info("Burst windows detected: %d", len(bot_bursts))

    # ── 2. Repo purpose analysis ──────────────────────────────────────────────
    logger.info("=== Repo Purpose Analysis ===")
    analyser = RepoPurposeAnalyser(df_stats, enriched_df=enriched_df, n_clusters=n_clusters)
    df_with_purpose = analyser.cluster_by_description()
    df_with_purpose = analyser.name_heuristic_classify()
    bot_heavy_repos = analyser.bot_heavy_repo_purposes(df_events)

    # ── 3. Bot-repo correlation ───────────────────────────────────────────────
    logger.info("=== Bot-Repo Correlation ===")
    correlator = BotRepoCorrelation(df_events, df_with_purpose)
    xtab         = correlator.bot_category_x_purpose()
    top_repos_per_bot = correlator.top_repos_per_bot()

    # ── 4. Anomaly detection ──────────────────────────────────────────────────
    logger.info("=== Anomaly Detection ===")
    detector  = AnomalyDetector(df_stats)
    anomalies = detector.zscore_anomalies()
    logger.info("Anomalous repos: %d", len(anomalies))

    # ── 5. Export ─────────────────────────────────────────────────────────────
    logger.info("=== Exporting Reports ===")
    exporter = ReportExporter()

    exporter.save_csv(bot_profile,        "bot_profiles")
    exporter.save_csv(bot_cat_summ,       "bot_category_summary")
    exporter.save_csv(bot_heavy_repos,    "bot_heavy_repos")
    exporter.save_csv(xtab.reset_index(), "bot_repo_correlation")
    exporter.save_csv(anomalies,          "anomalous_repos")
    exporter.save_csv(top_repos_per_bot,  "top_repos_per_bot")
    if not bot_bursts.empty:
        exporter.save_csv(bot_bursts, "bot_bursts")

    exporter.save_html_report(
        bot_profile=bot_profile,
        repo_purposes=bot_heavy_repos,
        correlation_xtab=xtab,
        anomalies=anomalies,
    )

    logger.info("Analytics complete. Reports in: %s", REPORTS_DIR)


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run bot + repo analytics")
    parser.add_argument("--prefix",    default="events",
                        help="Parquet file prefix (default: events)")
    parser.add_argument("--no-enrich", action="store_true",
                        help="Skip GitHub API enrichment")
    parser.add_argument("--max-enrich", type=int, default=300,
                        help="Max repos to enrich via GitHub API")
    parser.add_argument("--clusters",  type=int, default=8,
                        help="Number of KMeans clusters")
    args = parser.parse_args()

    run_analytics(
        events_prefix=args.prefix,
        enrich_with_github=not args.no_enrich,
        max_enrich_repos=args.max_enrich,
        n_clusters=args.clusters,
    )
