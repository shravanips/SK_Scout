"""
analytics.py
------------
Full analysis pipeline. Every class documents its origin.

Source breakdown
----------------
Kept from Kanak (unchanged logic, field names aligned)
  - BotActorProfiler        - known-bot profiling, burst detection
  - RepoPurposeAnalyser     - TF-IDF + KMeans description clustering,
                              language distribution, topic frequency,
                              name heuristics, bot-heavy repo filtering
  - BotRepoCorrelation      - cross-tab of bot category x repo purpose
  - AnomalyDetector         - Z-score outlier detection on repo features

New from Shravani
  - SuspiciousHumanAnalyser - profiles non-bot accounts that behave like bots
  - LockstepAnalyser        - examines coordinated multi-account activity
  - PhishingRepoAnalyser    - name patterns, branch explosion, AI co-authors

ReportExporter
  - save_csv            : Kanak (empty-df guard added by Shravani)
  - save_html_report    : Shravani's dark-theme design (Space Mono + Syne,
                          summary stat bar, NEW badges), but now also
                          includes Kanak's BotRepoCorrelation section
  - accepts processed_dir / reports_dir params (Shravani's per-run layout)

run_analytics
  - Full orchestration of all classes from both contributors
  - Optional GitHub API enrichment preserved from Kanak
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

from config import (
    BOT_TAXONOMY,
    DEFAULT_PARAMS,
    PATHS,
    REPO_NAME_CATEGORIES,
)
from utils import (
    clean_description,
    ensure_dir,
    enrich_repos_with_github_api,
    load_parquet,
    save_parquet,
    setup_logging,
)

logger = logging.getLogger(__name__)

PROCESSED_DIR = PATHS.PROCESSED
REPORTS_DIR   = PATHS.REPORTS


# -- bot taxonomy helper -------------------------------------------------------
def classify_bot(login: str) -> str:
    """Return a functional category for a bot login. Source: Kanak + Shravani."""
    lo = login.lower()
    for category, keywords in BOT_TAXONOMY.items():
        if any(kw in lo for kw in keywords):
            return category
    return "other"


# -- 1. BotActorProfiler (Kanak) -----------------------------------------------
class BotActorProfiler:
    """
    Analyse the behaviour of known-bot actors in the event stream.
    Source: Kanak. Field name aligned: is_bot_actor.
    """

    def __init__(self, events_df: pd.DataFrame):
        self.df = events_df[events_df["is_bot_actor"]].copy()
        if self.df.empty:
            logger.warning("No bot events found in the dataset.")

    def profile(self) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()

        p = self.df.groupby("actor_login", sort=False).agg(
            total_events        =("event_id",   "count"),
            unique_repos        =("repo_name",  "nunique"),
            event_type_diversity=("event_type", "nunique"),
            first_seen          =("created_at", "min"),
            last_seen           =("created_at", "max"),
        ).reset_index()

        p["activity_span_h"] = (
            (p["last_seen"] - p["first_seen"]).dt.total_seconds() / 3600
        )
        p["events_per_hour"] = (
            p["total_events"] / p["activity_span_h"].replace(0, 1)
        )
        p["bot_category"]  = p["actor_login"].apply(classify_bot)
        p["wide_footprint"] = p["unique_repos"] >= DEFAULT_PARAMS.WIDE_FOOTPRINT_REPOS
        return p.sort_values("total_events", ascending=False)

    def event_type_breakdown(self) -> pd.DataFrame:
        """Cross-tab: bot_login x event_type counts. Source: Kanak."""
        if self.df.empty:
            return pd.DataFrame()
        return pd.crosstab(self.df["actor_login"], self.df["event_type"])

    def category_summary(self) -> pd.DataFrame:
        p = self.profile()
        if p.empty:
            return pd.DataFrame()
        return p.groupby("bot_category").agg(
            n_bots            =("actor_login",    "count"),
            total_events      =("total_events",   "sum"),
            median_repos      =("unique_repos",   "median"),
            wide_footprint_pct=("wide_footprint", "mean"),
        ).reset_index()

    def detect_bursts(
        self,
        window_minutes: int = DEFAULT_PARAMS.BURST_WINDOW_MIN,
        threshold:      int = DEFAULT_PARAMS.BURST_THRESHOLD,
    ) -> pd.DataFrame:
        """Sliding-window burst detection. Source: Kanak."""
        if self.df.empty:
            return pd.DataFrame()
        tmp = self.df.set_index("created_at").sort_index()
        results = []
        for bot, grp in tmp.groupby("actor_login"):
            counts = grp["event_id"].resample(f"{window_minutes}min").count()
            for ts, cnt in counts[counts >= threshold].items():
                results.append({
                    "actor_login": bot,
                    "window_start": ts,
                    "event_count": cnt,
                })
        return pd.DataFrame(results)


# -- 2. SuspiciousHumanAnalyser (Shravani) -------------------------------------
class SuspiciousHumanAnalyser:
    """
    Profiles non-bot accounts that exhibit bot-like behaviour:
    low entropy, burst activity, single-event-type focus, AI co-authorship.
    Source: Shravani. Consumes actor_stats_df output from ingest.py.
    """

    def __init__(self, actor_stats_df: pd.DataFrame):
        self.df = actor_stats_df[~actor_stats_df["is_bot_actor"]].copy()

    def top_suspicious(
        self,
        min_score: int = DEFAULT_PARAMS.SUSP_HUMAN_MIN_SCORE,
        top_n:     int = DEFAULT_PARAMS.SUSP_HUMAN_TOP_N,
    ) -> pd.DataFrame:
        if self.df.empty or "suspicious_human_score" not in self.df.columns:
            return pd.DataFrame()
        return (
            self.df[self.df["suspicious_human_score"] >= min_score]
            .sort_values("suspicious_human_score", ascending=False)
            .head(top_n)
        )

    def score_distribution(self) -> pd.Series:
        if "suspicious_human_score" not in self.df.columns:
            return pd.Series(dtype=int)
        return self.df["suspicious_human_score"].value_counts().sort_index()

    def ai_coauthor_accounts(self) -> pd.DataFrame:
        if "ai_coauthor" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["ai_coauthor"]].sort_values("total_events", ascending=False)


# -- 3. RepoPurposeAnalyser (Kanak) --------------------------------------------
class RepoPurposeAnalyser:
    """
    Classify / cluster repos by purpose using:
      - GitHub API metadata (language, topics, description) -- Kanak
      - TF-IDF + KMeans unsupervised clustering              -- Kanak
      - Rule-based heuristics from repo name patterns        -- Kanak + Shravani (added 'suspicious' category)
    Source: Kanak. Shravani added the 'suspicious' name heuristic (crack/free/hack/...).
    """

    def __init__(
        self,
        repo_stats_df: pd.DataFrame,
        enriched_df: Optional[pd.DataFrame] = None,
        n_clusters: int = DEFAULT_PARAMS.N_CLUSTERS,
    ):
        self.stats    = repo_stats_df.copy()
        self.enriched = enriched_df
        self.n_clusters = n_clusters
        self._merged: Optional[pd.DataFrame] = None

    def _get_merged(self) -> pd.DataFrame:
        if self._merged is not None:
            return self._merged
        self._merged = (
            self.stats.merge(self.enriched, on="repo_name", how="left")
            if self.enriched is not None and not self.enriched.empty
            else self.stats.copy()
        )
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
        """TF-IDF + KMeans clustering of repo descriptions. Source: Kanak."""
        df = self._get_merged().copy()
        if "description" not in df.columns or df["description"].isna().all():
            logger.warning("No description data; skipping clustering.")
            df["purpose_cluster"] = -1
            df["purpose_label"]   = "unknown"
            return df

        texts      = df["description"].fillna("").apply(clean_description)
        valid_mask = texts.str.len() > 5

        if valid_mask.sum() < self.n_clusters:
            logger.warning(
                "Too few valid descriptions (%d) for %d clusters.",
                valid_mask.sum(), self.n_clusters,
            )
            df["purpose_cluster"] = -1
            df["purpose_label"]   = "unknown"
            return df

        vec = TfidfVectorizer(
            max_features=DEFAULT_PARAMS.TFIDF_MAX_FEATURES,
            stop_words="english",
            ngram_range=(1, 2),
            min_df=DEFAULT_PARAMS.TFIDF_MIN_DF,
        )
        try:
            X = vec.fit_transform(texts[valid_mask])
        except ValueError:
            # min_df pruned all terms (corpus too small) -- retry with min_df=1
            logger.warning(
                "TF-IDF pruned all terms with min_df=%d; retrying with min_df=1.",
                DEFAULT_PARAMS.TFIDF_MIN_DF,
            )
            vec = TfidfVectorizer(
                max_features=DEFAULT_PARAMS.TFIDF_MAX_FEATURES,
                stop_words="english",
                ngram_range=(1, 2),
                min_df=1,
            )
            try:
                X = vec.fit_transform(texts[valid_mask])
            except ValueError:
                logger.warning("TF-IDF failed even with min_df=1; skipping clustering.")
                df["purpose_cluster"] = -1
                df["purpose_label"]   = "unknown"
                self._merged = df
                return df
        km = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        labels = km.fit_predict(X)

        df.loc[valid_mask, "purpose_cluster"] = labels.astype(int)
        df["purpose_cluster"] = df["purpose_cluster"].fillna(-1).astype(int)

        terms = vec.get_feature_names_out()
        order = km.cluster_centers_.argsort()[:, ::-1]
        cluster_labels = {i: "_".join(terms[order[i, :3]]) for i in range(self.n_clusters)}
        cluster_labels[-1] = "no_description"
        df["purpose_label"] = df["purpose_cluster"].map(cluster_labels)

        self._merged = df
        return df

    def name_heuristic_classify(self) -> pd.DataFrame:
        """
        Rule-based classifier from repo name.
        Source: Kanak. Shravani added the 'suspicious' category
        (crack/free/hack/wallet/stealer/cheat).
        Patterns now read from config.REPO_NAME_CATEGORIES.
        """
        df   = self._get_merged().copy()
        name = df["repo_name"].str.lower()

        conditions = [name.str.contains(pat, regex=True) for pat in REPO_NAME_CATEGORIES]
        choices    = list(REPO_NAME_CATEGORIES.values())
        df["name_category"] = np.select(conditions, choices, default="general")
        self._merged = df
        return df

    def bot_heavy_repo_purposes(
        self,
        events_df: pd.DataFrame,
        bot_ratio_threshold: float = DEFAULT_PARAMS.BOT_HEAVY_THRESHOLD,
    ) -> pd.DataFrame:
        """Subset repos where >50% of events come from bots. Source: Kanak."""
        df = self._get_merged()
        if "bot_ratio" not in df.columns:
            logger.warning("bot_ratio column missing.")
            return pd.DataFrame()
        bot_heavy = df[df["bot_ratio"] >= bot_ratio_threshold].copy()
        logger.info("%d bot-heavy repos (bot_ratio >= %.0f%%)",
                    len(bot_heavy), bot_ratio_threshold * 100)
        return bot_heavy


# -- 4. BotRepoCorrelation (Kanak) ---------------------------------------------
class BotRepoCorrelation:
    """
    Link bot categories to repo purpose clusters.
    Source: Kanak. Not present in Shravani's version -- restored here.
    """

    def __init__(self, events_df: pd.DataFrame, repo_purpose_df: pd.DataFrame):
        self.events    = events_df.copy()
        self.repo_meta = repo_purpose_df[
            ["repo_name"] +
            [c for c in ["purpose_label", "name_category", "language"]
             if c in repo_purpose_df.columns]
        ].copy()

    def bot_category_x_purpose(self) -> pd.DataFrame:
        bots = self.events[self.events["is_bot_actor"]].copy()
        bots["bot_category"] = bots["actor_login"].apply(classify_bot)
        merged = bots.merge(self.repo_meta, on="repo_name", how="left")
        if "purpose_label" not in merged.columns:
            merged["purpose_label"] = "unknown"
        return pd.crosstab(merged["bot_category"], merged["purpose_label"], margins=True)

    def top_repos_per_bot(self, top_n: int = 5) -> pd.DataFrame:
        bots   = self.events[self.events["is_bot_actor"]]
        counts = (
            bots.groupby(["actor_login", "repo_name"])
            .size()
            .reset_index(name="event_count")
        )
        counts["rank"] = counts.groupby("actor_login")["event_count"].rank(
            ascending=False, method="first"
        )
        return (
            counts[counts["rank"] <= top_n]
            .merge(self.repo_meta, on="repo_name", how="left")
            .sort_values(["actor_login", "rank"])
        )


# -- 5. LockstepAnalyser (Shravani) --------------------------------------------
class LockstepAnalyser:
    """
    Examines cross-repo coordinated-activity windows detected in ingest.py.
    Source: Shravani.
    """

    def __init__(self, lockstep_df: pd.DataFrame):
        self.df = lockstep_df.copy() if not lockstep_df.empty else pd.DataFrame()

    def top_targeted_repos(self, top_n: int = 20) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()
        return (
            self.df.groupby("repo_name").agg(
                lockstep_windows=("window_start", "count"),
                max_actors      =("actor_count",  "max"),
                total_events    =("event_count",  "sum"),
            )
            .reset_index()
            .sort_values("lockstep_windows", ascending=False)
            .head(top_n)
        )

    def repeated_actor_clusters(self, min_appearances: int = 2) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()
        cluster_counts = (
            self.df.groupby("actors")["repo_name"]
            .nunique()
            .reset_index()
            .rename(columns={"repo_name": "repos_hit"})
        )
        cluster_counts["appearances"] = self.df.groupby("actors").size().values
        return cluster_counts[
            cluster_counts["appearances"] >= min_appearances
        ].sort_values("repos_hit", ascending=False)


# -- 6. PhishingRepoAnalyser (Shravani) ----------------------------------------
class PhishingRepoAnalyser:
    """
    Scores repos on name patterns, branch explosion, and AI co-author signals.
    Source: Shravani.
    """

    def __init__(self, repo_stats_df: pd.DataFrame):
        self.df = repo_stats_df.copy()

    def high_risk_repos(self, min_score: int = 3) -> pd.DataFrame:
        if "suspicious_score" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["suspicious_score"] >= min_score].sort_values(
            "suspicious_score", ascending=False
        )

    def phish_name_repos(self) -> pd.DataFrame:
        if "phish_name_flag" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["phish_name_flag"]].sort_values("suspicious_score", ascending=False)

    def branch_explosion_repos(
        self, threshold: int = DEFAULT_PARAMS.BRANCH_EXPLOSION_THRESH
    ) -> pd.DataFrame:
        if "distinct_branches" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["distinct_branches"] >= threshold].sort_values(
            "distinct_branches", ascending=False
        )

    def ai_coauthor_repos(self) -> pd.DataFrame:
        if "ai_coauthor_flag" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["ai_coauthor_flag"]].sort_values("suspicious_score", ascending=False)

    def risk_breakdown(self) -> pd.DataFrame:
        rows = []
        checks = [
            ("phish_name_flag",  "Suspicious repo name keyword"),
            ("ai_coauthor_flag", "AI handle in commit co-author"),
        ]
        for col, label in checks:
            if col in self.df.columns:
                rows.append({"signal": label, "repo_count": int(self.df[col].sum())})
        if "distinct_branches" in self.df.columns:
            rows.append({
                "signal": "Branch explosion (>50 branches)",
                "repo_count": int((self.df["distinct_branches"] >= 50).sum()),
            })
        if "bot_ratio" in self.df.columns:
            rows.append({
                "signal": "Bot-ratio > 0.5",
                "repo_count": int((self.df["bot_ratio"] > 0.5).sum()),
            })
        return pd.DataFrame(rows)


# -- 7. AnomalyDetector (Kanak + Shravani) ------------------------------------
class AnomalyDetector:
    """
    Z-score outlier detection on repo-level numeric features.
    Source: Kanak. Shravani added 'distinct_branches' to FEATURE_COLS.
    """

    FEATURE_COLS = [
        "total_events", "unique_actors", "bot_ratio",
        "events_per_actor", "events_per_second",
        "distinct_branches",   # Shravani addition
    ]

    def __init__(
        self,
        repo_stats_df: pd.DataFrame,
        z_threshold: float = DEFAULT_PARAMS.Z_SCORE_THRESHOLD,
    ):
        self.stats      = repo_stats_df.copy()
        self.z_threshold = z_threshold

    def zscore_anomalies(self) -> pd.DataFrame:
        available = [c for c in self.FEATURE_COLS if c in self.stats.columns]
        if not available:
            return pd.DataFrame()
        scaler  = StandardScaler()
        numeric = self.stats[available].fillna(0)
        zdf = pd.DataFrame(
            scaler.fit_transform(numeric),
            columns=[f"z_{c}" for c in available],
            index=self.stats.index,
        )
        combined = pd.concat([self.stats[["repo_name"] + available], zdf], axis=1)
        combined["max_z"]          = zdf.abs().max(axis=1)
        combined["anomaly_feature"] = zdf.abs().idxmax(axis=1).str.replace("z_", "", regex=False)
        return combined[combined["max_z"] > self.z_threshold].sort_values("max_z", ascending=False)


# -- report helper functions (module-level, no f-string interference) ---------

def _filter_cols(df: "pd.DataFrame", wanted: list) -> "pd.DataFrame":
    """Return df keeping only wanted columns that actually exist."""
    cols = [c for c in wanted if c in df.columns]
    return df[cols] if cols else df


def _df_to_html(df: "pd.DataFrame", n: int = 30, wanted_cols: list = None) -> str:
    """Render a DataFrame as a clean HTML table, safe for embedding in f-strings."""
    if df is None or df.empty:
        return "<p class='empty'>No data for this window.</p>"
    if wanted_cols:
        df = _filter_cols(df, wanted_cols)
    df = df.head(n).copy()
    for col in df.select_dtypes(include="float").columns:
        df[col] = df[col].round(3)
    bool_map = {True: "Yes", False: "No"}
    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].map(bool_map)
    return df.to_html(index=False, border=0, classes="tbl", na_rep="-", escape=True)


def _bar_chart_html(df: "pd.DataFrame", x: str, y: str, title: str) -> str:
    """Render a Plotly bar chart as an HTML snippet, or empty string if unavailable."""
    try:
        import plotly.express as px
        import plotly.io as pio
    except ImportError:
        return ""
    if df is None or df.empty:
        return ""
    if x not in df.columns or y not in df.columns:
        return ""
    fig = px.bar(
        df.head(20), x=x, y=y, title=title,
        color_discrete_sequence=["#e94560", "#533483", "#0f3460"],
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e2e8f0",
        font_family="Space Mono",
        title_font_size=13,
        margin=dict(l=8, r=8, t=36, b=8),
        xaxis=dict(gridcolor="#1e2d45", tickfont=dict(size=10)),
        yaxis=dict(gridcolor="#1e2d45", tickfont=dict(size=10)),
    )
    cfg = {"displayModeBar": False}
    return pio.to_html(fig, full_html=False, config=cfg)


# -- 8. ReportExporter --------------------------------------------------------
class ReportExporter:
    """
    Save results to CSV and a self-contained HTML report.

    save_csv        : Kanak's logic + Shravani's empty-df guard.
    save_html_report: Shravani's dark-theme design (Space Mono + Syne,
                      summary stat bar, NEW/ANOMALY badges).
                      Extended with Kanak's BotRepoCorrelation section.
    """

    def __init__(self, output_dir: Path = REPORTS_DIR):
        self.out = ensure_dir(Path(output_dir))

    def save_csv(self, df: pd.DataFrame, name: str) -> Optional[Path]:
        if df is None or df.empty:
            return None
        path = self.out / f"{name}.csv"
        df.to_csv(path, index=False)
        logger.info("CSV -> %s", path)
        return path

    def save_html_report(
        self,
        bot_profile:       pd.DataFrame,
        suspicious_humans: pd.DataFrame,
        lockstep_repos:    pd.DataFrame,
        phish_repos:       pd.DataFrame,
        risk_breakdown:    pd.DataFrame,
        anomalies:         pd.DataFrame,
        bot_cat_summary:   pd.DataFrame,
        branch_explosion:  pd.DataFrame,
        ai_coauthor_repos: pd.DataFrame,
        correlation_xtab:  pd.DataFrame,   # Kanak: BotRepoCorrelation
        total_repos: int = 0,
    ) -> Path:
        """Generate self-contained HTML with Shravani's design + Kanak's correlation section."""
        # column display config -- which cols to show per section
        REPO_COLS    = ["repo_name", "total_events", "unique_actors", "bot_events",
                        "bot_ratio", "phish_name_flag", "ai_coauthor_flag",
                        "distinct_branches", "suspicious_score"]
        HUMAN_COLS   = ["actor_login", "total_events", "unique_repos",
                        "event_entropy", "burst_fraction", "suspicious_human_score"]
        LOCK_COLS    = ["repo_name", "window_start", "actor_count",
                        "event_count", "event_types"]
        BRANCH_COLS  = ["repo_name", "total_events", "unique_actors", "bot_ratio",
                        "distinct_branches", "suspicious_score"]
        ANOM_COLS    = ["repo_name", "total_events", "bot_ratio",
                        "events_per_actor", "distinct_branches", "max_z", "anomaly_feature"]
        BOT_CAT_COLS = ["bot_category", "n_bots", "total_events",
                        "median_repos", "wide_footprint_pct"]
        RISK_COLS    = ["signal", "repo_count"]
        AI_COLS      = ["repo_name", "total_events", "bot_ratio",
                        "ai_coauthor_flag", "suspicious_score"]

        n_phish       = len(phish_repos)       if phish_repos is not None       and not phish_repos.empty       else 0
        n_susp_humans = len(suspicious_humans) if suspicious_humans is not None and not suspicious_humans.empty else 0
        n_lockstep    = len(lockstep_repos)    if lockstep_repos is not None    and not lockstep_repos.empty    else 0
        n_bots        = len(bot_profile)       if bot_profile is not None       and not bot_profile.empty       else 0
        n_anomalies   = len(anomalies)         if anomalies is not None         and not anomalies.empty         else 0
        pct_risk      = (n_phish / total_repos * 100) if total_repos > 0 else 0

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GitGub - GitHub Anomaly &amp; Bot Detection Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
/* ── reset ───────────────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

/* ── tokens ──────────────────────────────────────────────── */
:root {{
  --bg:       #0a0e1a;
  --surface:  #111827;
  --surface2: #0f1a2e;
  --border:   #1e2d45;
  --accent:   #e94560;
  --blue:     #0f3460;
  --purple:   #533483;
  --text:     #e2e8f0;
  --muted:    #94a3b8;
  --green:    #10b981;
  --yellow:   #f59e0b;
  --radius:   10px;
  --font-mono: 'Space Mono', ui-monospace, monospace;
  --font-sans: 'Syne', system-ui, sans-serif;
}}

/* ── base ────────────────────────────────────────────────── */
body {{
  background: var(--bg);
  color: var(--text);
  font-family: var(--font-mono);
  font-size: 13px;
  line-height: 1.6;
  min-height: 100vh;
}}

/* ── header ──────────────────────────────────────────────── */
header {{
  background: linear-gradient(135deg, #0f3460 0%, #533483 55%, #e94560 100%);
  padding: 44px 40px 32px;
}}
h1 {{
  font-family: var(--font-sans);
  font-size: 2.2rem;
  font-weight: 800;
  letter-spacing: -1px;
  color: #fff;
}}
h1 span {{ color: #fbbf24; }}
.subtitle {{
  color: rgba(255,255,255,0.65);
  font-size: 0.82rem;
  margin-top: 6px;
}}

/* ── stat bar ────────────────────────────────────────────── */
.stat-bar {{
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 28px;
}}
.stat {{
  background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: var(--radius);
  padding: 12px 20px;
  min-width: 130px;
  flex: 1 1 130px;
}}
.stat .val {{
  font-family: var(--font-sans);
  font-size: 1.7rem;
  font-weight: 700;
  color: #fbbf24;
  line-height: 1;
}}
.stat .lbl {{
  font-size: 0.68rem;
  color: rgba(255,255,255,0.55);
  margin-top: 4px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}}

/* ── main layout ─────────────────────────────────────────── */
main {{
  max-width: 1400px;
  margin: 0 auto;
  padding: 36px 28px 60px;
}}
section {{
  margin-bottom: 48px;
}}

/* ── section header ──────────────────────────────────────── */
.section-header {{
  display: flex;
  align-items: center;
  gap: 10px;
  padding-bottom: 12px;
  margin-bottom: 18px;
  border-bottom: 1px solid var(--border);
}}
h2 {{
  font-family: var(--font-sans);
  font-size: 1.1rem;
  font-weight: 700;
  color: var(--text);
}}

/* ── badges ──────────────────────────────────────────────── */
.badge {{
  display: inline-block;
  padding: 3px 9px;
  border-radius: 4px;
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 1px;
  text-transform: uppercase;
  background: var(--accent);
  color: #fff;
  flex-shrink: 0;
}}
.badge.new   {{ background: var(--green); }}
.badge.warn  {{ background: var(--yellow); color: #000; }}
.badge.kanak {{ background: var(--blue); }}
.badge.stats {{ background: var(--purple); }}

/* ── card ────────────────────────────────────────────────── */
.card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 0;
  overflow: hidden;
}}
.card-padded {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 20px;
  overflow: hidden;
}}

/* ── grid ────────────────────────────────────────────────── */
.grid2 {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  align-items: start;
}}
@media (max-width: 900px) {{
  .grid2 {{ grid-template-columns: 1fr; }}
}}

/* ── table wrapper — horizontal scroll only ──────────────── */
.tbl-wrap {{
  width: 100%;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}}

/* ── table ───────────────────────────────────────────────── */
.tbl {{
  width: 100%;
  border-collapse: collapse;
  font-size: 0.75rem;
  white-space: nowrap;          /* keep all cells on one line */
  table-layout: auto;           /* let browser size cols to content */
}}
.tbl thead tr {{
  background: var(--blue);
}}
.tbl th {{
  padding: 10px 14px;
  text-align: left;
  font-weight: 700;
  font-size: 0.7rem;
  color: #fff;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  border-right: 1px solid rgba(255,255,255,0.06);
  position: sticky;
  top: 0;
}}
.tbl th:last-child {{ border-right: none; }}

.tbl tbody tr:nth-child(even) {{
  background: var(--surface2);
}}
.tbl tbody tr:hover td {{
  background: rgba(255,255,255,0.04);
  color: var(--text);
}}
.tbl td {{
  padding: 8px 14px;
  border-bottom: 1px solid var(--border);
  border-right: 1px solid rgba(255,255,255,0.03);
  color: var(--muted);
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.tbl td:first-child {{
  color: var(--text);
  font-weight: 700;
  max-width: 280px;
}}
.tbl td:last-child {{ border-right: none; }}

/* ── empty state ─────────────────────────────────────────── */
p.empty {{
  padding: 24px 20px;
  color: var(--muted);
  font-size: 0.8rem;
  font-style: italic;
}}
</style>
</head>
<body>

<header>
  <h1>Git<span>Gub</span></h1>
  <p class="subtitle">GitHub Anomaly &amp; Bot Detection Report</p>
  <div class="stat-bar">
    <div class="stat"><div class="val">{total_repos:,}</div><div class="lbl">Total Repos</div></div>
    <div class="stat"><div class="val">{n_phish:,}</div><div class="lbl">High-Risk ({pct_risk:.1f}%)</div></div>
    <div class="stat"><div class="val">{n_susp_humans:,}</div><div class="lbl">Suspicious Humans</div></div>
    <div class="stat"><div class="val">{n_lockstep:,}</div><div class="lbl">Lockstep Targets</div></div>
    <div class="stat"><div class="val">{n_bots:,}</div><div class="lbl">Known Bots</div></div>
    <div class="stat"><div class="val">{n_anomalies:,}</div><div class="lbl">Z-Score Anomalies</div></div>
  </div>
</header>

<main>

<!-- Risk Signal Breakdown -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>Risk Signal Breakdown</h2>
  </div>
  <div class="grid2">
    <div class="card"><div class="tbl-wrap">{_df_to_html(risk_breakdown, wanted_cols=RISK_COLS)}</div></div>
    <div class="card-padded">{_bar_chart_html(risk_breakdown, "signal", "repo_count", "Repos Flagged per Signal")}</div>
  </div>
</section>

<!-- High-Risk / Phishing Repos -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>High-Risk / Phishing Repos</h2>
  </div>
  <div class="card"><div class="tbl-wrap">{_df_to_html(phish_repos, wanted_cols=REPO_COLS)}</div></div>
</section>

<!-- Suspicious Human Accounts -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>Suspicious Human Accounts (no [bot] tag)</h2>
  </div>
  <div class="grid2">
    <div class="card"><div class="tbl-wrap">{_df_to_html(suspicious_humans, wanted_cols=HUMAN_COLS)}</div></div>
    <div class="card-padded">{_bar_chart_html(suspicious_humans.head(15) if suspicious_humans is not None and not suspicious_humans.empty else pd.DataFrame(), "actor_login", "suspicious_human_score", "Top Suspicious Accounts by Score")}</div>
  </div>
</section>

<!-- Lockstep -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>Lockstep-Targeted Repos (Coordinated Activity)</h2>
  </div>
  <div class="card"><div class="tbl-wrap">{_df_to_html(lockstep_repos, wanted_cols=LOCK_COLS)}</div></div>
</section>

<!-- Branch Explosion -->
<section>
  <div class="section-header">
    <span class="badge warn">ANOMALY</span>
    <h2>Branch Explosion Repos</h2>
  </div>
  <div class="card"><div class="tbl-wrap">{_df_to_html(branch_explosion, wanted_cols=BRANCH_COLS)}</div></div>
</section>

<!-- AI Co-Author -->
<section>
  <div class="section-header">
    <span class="badge warn">ANOMALY</span>
    <h2>Repos with AI Handle in Commit Co-Authors</h2>
  </div>
  <div class="card"><div class="tbl-wrap">{_df_to_html(ai_coauthor_repos, wanted_cols=AI_COLS)}</div></div>
</section>

<!-- Known Bots -->
<section>
  <div class="section-header">
    <span class="badge">BOTS</span>
    <h2>Known Bot Profiles</h2>
  </div>
  <div class="grid2">
    <div class="card"><div class="tbl-wrap">{_df_to_html(bot_cat_summary, wanted_cols=BOT_CAT_COLS)}</div></div>
    <div class="card-padded">{_bar_chart_html(bot_cat_summary, "bot_category", "total_events", "Events by Bot Category")}</div>
  </div>
</section>

<!-- Bot x Repo Correlation -->
<section>
  <div class="section-header">
    <span class="badge kanak">KANAK</span>
    <h2>Bot Category x Repo Purpose Correlation</h2>
  </div>
  <div class="card"><div class="tbl-wrap">{_df_to_html(correlation_xtab.reset_index() if correlation_xtab is not None and not correlation_xtab.empty else pd.DataFrame())}</div></div>
</section>

<!-- Z-Score Anomalies -->
<section>
  <div class="section-header">
    <span class="badge stats">STATS</span>
    <h2>Statistical Anomalies (Z-Score)</h2>
  </div>
  <div class="card"><div class="tbl-wrap">{_df_to_html(anomalies, wanted_cols=ANOM_COLS)}</div></div>
</section>

</main>
</body>
</html>"""

        path = self.out / "report.html"
        path.write_text(html, encoding="utf-8")
        logger.info("HTML report -> %s", path)
        return path


# -- orchestration -------------------------------------------------------------
def run_analytics(
    events_prefix:      str  = "events",
    enrich_with_github: bool = True,          # Kanak feature -- preserved
    max_enrich_repos:   int  = DEFAULT_PARAMS.MAX_ENRICH_REPOS,
    n_clusters:         int  = DEFAULT_PARAMS.N_CLUSTERS,
    processed_dir: Optional[Path] = None,    # Shravani: per-run folder support
    reports_dir:   Optional[Path] = None,    # Shravani: per-run folder support
) -> None:
    setup_logging()
    processed_dir = Path(processed_dir) if processed_dir is not None else PROCESSED_DIR
    reports_dir   = Path(reports_dir)   if reports_dir   is not None else REPORTS_DIR
    ensure_dir(reports_dir)

    def _load(name: str) -> pd.DataFrame:
        p = processed_dir / name
        if not p.exists():
            logger.warning("Missing: %s", p)
            return pd.DataFrame()
        return pd.read_parquet(p)

    df_events   = _load(f"{events_prefix}_raw.parquet")
    df_repos    = _load(f"{events_prefix}_repo_stats.parquet")
    df_actors   = _load(f"{events_prefix}_actor_stats.parquet")
    df_lockstep = _load(f"{events_prefix}_lockstep.parquet")

    # -- Optional GitHub API enrichment (Kanak) --------------------------------
    enriched_df   = None
    enriched_path = processed_dir / "repo_metadata.parquet"
    if enrich_with_github:
        if enriched_path.exists():
            logger.info("Loading cached repo metadata.")
            enriched_df = load_parquet(enriched_path)
        else:
            top_repos   = df_repos["repo_name"].head(max_enrich_repos).tolist()
            enriched_df = enrich_repos_with_github_api(top_repos, max_repos=max_enrich_repos)
            if enriched_df is not None and not enriched_df.empty:
                save_parquet(enriched_df, enriched_path)

    # -- 1. Bot profiling (Kanak) ----------------------------------------------
    profiler     = BotActorProfiler(df_events)
    bot_profile  = profiler.profile()
    bot_bursts   = profiler.detect_bursts()
    bot_cat_summ = profiler.category_summary()

    # -- 2. Suspicious humans (Shravani) ---------------------------------------
    sha         = SuspiciousHumanAnalyser(df_actors)
    susp_humans = sha.top_suspicious()
    ai_accounts = sha.ai_coauthor_accounts()

    # -- 3. Repo purpose analysis (Kanak) --------------------------------------
    analyser       = RepoPurposeAnalyser(df_repos, enriched_df=enriched_df, n_clusters=n_clusters)
    df_with_purpose = analyser.cluster_by_description()
    df_with_purpose = analyser.name_heuristic_classify()
    bot_heavy_repos = analyser.bot_heavy_repo_purposes(df_events)

    # -- 4. Bot<->Repo correlation (Kanak) ---------------------------------------
    correlator       = BotRepoCorrelation(df_events, df_with_purpose)
    xtab             = correlator.bot_category_x_purpose()
    top_repos_per_bot = correlator.top_repos_per_bot()

    # -- 5. Lockstep analysis (Shravani) ---------------------------------------
    lsa           = LockstepAnalyser(df_lockstep)
    lockstep_repos = lsa.top_targeted_repos()

    # -- 6. Phishing repo analysis (Shravani) ----------------------------------
    pra          = PhishingRepoAnalyser(df_repos)
    high_risk    = pra.high_risk_repos()
    phish_names  = pra.phish_name_repos()
    branch_exp   = pra.branch_explosion_repos()
    ai_repo      = pra.ai_coauthor_repos()
    risk_brkdwn  = pra.risk_breakdown()

    # -- 7. Anomaly detection (Kanak + Shravani) -------------------------------
    anomalies = AnomalyDetector(df_repos).zscore_anomalies()

    # -- 8. Export -------------------------------------------------------------
    exporter = ReportExporter(output_dir=reports_dir)

    # Kanak CSVs
    exporter.save_csv(bot_profile,        "bot_profiles")
    exporter.save_csv(bot_cat_summ,       "bot_category_summary")
    exporter.save_csv(bot_heavy_repos,    "bot_heavy_repos")
    exporter.save_csv(xtab.reset_index(), "bot_repo_correlation")
    exporter.save_csv(top_repos_per_bot,  "top_repos_per_bot")
    exporter.save_csv(anomalies,          "anomalous_repos")
    if bot_bursts is not None and not bot_bursts.empty:
        exporter.save_csv(bot_bursts,     "bot_bursts")

    # Shravani CSVs
    exporter.save_csv(susp_humans,   "suspicious_humans")
    exporter.save_csv(ai_accounts,   "ai_coauthor_accounts")
    exporter.save_csv(lockstep_repos,"lockstep_repos")
    exporter.save_csv(high_risk,     "high_risk_repos")
    exporter.save_csv(phish_names,   "phish_name_repos")
    exporter.save_csv(branch_exp,    "branch_explosion_repos")
    exporter.save_csv(ai_repo,       "ai_coauthor_repos")
    exporter.save_csv(risk_brkdwn,   "risk_breakdown")

    exporter.save_html_report(
        bot_profile       = bot_profile,
        suspicious_humans = susp_humans,
        lockstep_repos    = lockstep_repos,
        phish_repos       = high_risk,
        risk_breakdown    = risk_brkdwn,
        anomalies         = anomalies,
        bot_cat_summary   = bot_cat_summ,
        branch_explosion  = branch_exp,
        ai_coauthor_repos = ai_repo,
        correlation_xtab  = xtab,           # Kanak
        total_repos       = len(df_repos),
    )

    logger.info("Analytics complete -> %s", reports_dir)


# -- CLI -----------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run bot + repo analytics")
    parser.add_argument("--prefix",     default="events")
    parser.add_argument("--no-enrich",  action="store_true")
    parser.add_argument("--max-enrich", type=int, default=DEFAULT_PARAMS.MAX_ENRICH_REPOS)
    parser.add_argument("--clusters",   type=int, default=DEFAULT_PARAMS.N_CLUSTERS)
    args = parser.parse_args()

    run_analytics(
        events_prefix=args.prefix,
        enrich_with_github=not args.no_enrich,
        max_enrich_repos=args.max_enrich,
        n_clusters=args.clusters,
    )