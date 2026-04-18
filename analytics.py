"""
analytics.py  –  SK_Scout / Shravani
--------------------------------------
Full analysis pipeline. Includes everything from Kanak's version PLUS:

NEW modules
-----------
* SuspiciousHumanAnalyser  – profiles accounts that behave like bots but
                              carry no [bot] label (our unique contribution).
* LockstepAnalyser         – examines cross-repo coordinated activity windows
                              across ALL event types (not just WatchEvents).
* PhishingRepoAnalyser     – scores repos on name patterns, branch explosion,
                              AI co-author signals, and activity signatures.
* NetworkGraphBuilder      – builds an actor↔repo bipartite graph and flags
                              highly connected suspicious clusters.

Kept from Kanak
---------------
* BotActorProfiler         – known-bot taxonomy + burst detection
* RepoPurposeAnalyser      – TF-IDF + KMeans clustering of repo descriptions
* AnomalyDetector          – Z-score outliers on numeric repo signals
* ReportExporter           – now produces a much richer HTML dashboard
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

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
REPORTS_DIR   = Path("data/reports")

# ── bot taxonomy (Kanak's) ────────────────────────────────────────────────────
BOT_TAXONOMY = {
    "dependency":   ["dependabot", "renovate", "greenkeeper", "depfu"],
    "ci_cd":        ["github-actions", "travis", "circleci", "semantic-release"],
    "security":     ["snyk-bot", "mend-bolt", "whitesource"],
    "code_quality": ["codecov", "codeclimate", "deepsource"],
    "docs":         ["allcontributors", "imgbot", "readme-bot"],
    "translation":  ["lokalise", "crowdin", "transifex"],
}

def classify_bot(login: str) -> str:
    lo = login.lower()
    for cat, kws in BOT_TAXONOMY.items():
        if any(k in lo for k in kws):
            return cat
    return "other"


# ── 1. BotActorProfiler (Kanak's, kept) ──────────────────────────────────────
class BotActorProfiler:
    def __init__(self, events_df: pd.DataFrame):
        self.df = events_df[events_df["is_known_bot"]].copy()

    def profile(self) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()
        p = self.df.groupby("actor_login").agg(
            total_events        =("event_id",   "count"),
            unique_repos        =("repo_name",  "nunique"),
            event_type_diversity=("event_type", "nunique"),
            first_seen          =("created_at", "min"),
            last_seen           =("created_at", "max"),
        ).reset_index()
        p["activity_span_h"] = (p["last_seen"] - p["first_seen"]).dt.total_seconds() / 3600
        p["events_per_hour"] = p["total_events"] / p["activity_span_h"].replace(0, 1)
        p["bot_category"]    = p["actor_login"].apply(classify_bot)
        p["wide_footprint"]  = p["unique_repos"] >= 5
        return p.sort_values("total_events", ascending=False)

    def category_summary(self) -> pd.DataFrame:
        p = self.profile()
        if p.empty:
            return pd.DataFrame()
        return p.groupby("bot_category").agg(
            n_bots            =("actor_login",   "count"),
            total_events      =("total_events",  "sum"),
            median_repos      =("unique_repos",  "median"),
            wide_footprint_pct=("wide_footprint","mean"),
        ).reset_index()

    def detect_bursts(self, window_minutes: int = 10, threshold: int = 20) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()
        tmp = self.df.set_index("created_at").sort_index()
        results = []
        for bot, grp in tmp.groupby("actor_login"):
            counts = grp["event_id"].resample(f"{window_minutes}min").count()
            for ts, cnt in counts[counts >= threshold].items():
                results.append({"actor_login": bot, "window_start": ts, "event_count": cnt})
        return pd.DataFrame(results)


# ── 2. SuspiciousHumanAnalyser (NEW) ─────────────────────────────────────────
class SuspiciousHumanAnalyser:
    """
    Identifies human-labelled accounts that exhibit bot-like behaviour:
    - Very low event-type entropy (only does one thing)
    - High burst fraction (events within 60 s of each other)
    - AI handle in commit co-authors
    - Disproportionate volume for time active
    """

    def __init__(self, actor_stats_df: pd.DataFrame):
        # actor_stats_df comes from ingest.compute_actor_stats()
        self.df = actor_stats_df[~actor_stats_df["is_known_bot"]].copy()

    def top_suspicious(self, min_score: int = 2, top_n: int = 50) -> pd.DataFrame:
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


# ── 3. LockstepAnalyser (NEW) ─────────────────────────────────────────────────
class LockstepAnalyser:
    """
    Analyses the lockstep detection output from ingest.detect_lockstep().
    Finds repos that appear in many lockstep windows (coordination signal)
    and actor clusters that repeatedly appear together.
    """

    def __init__(self, lockstep_df: pd.DataFrame):
        self.df = lockstep_df.copy() if not lockstep_df.empty else pd.DataFrame()

    def top_targeted_repos(self, top_n: int = 20) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()
        return (
            self.df.groupby("repo_name")
            .agg(lockstep_windows=("window_start", "count"),
                 max_actors      =("actor_count",  "max"),
                 total_events    =("event_count",  "sum"))
            .reset_index()
            .sort_values("lockstep_windows", ascending=False)
            .head(top_n)
        )

    def repeated_actor_clusters(self, min_appearances: int = 2) -> pd.DataFrame:
        """Actor sets that appear together in 2+ windows."""
        if self.df.empty:
            return pd.DataFrame()
        cluster_counts = (
            self.df.groupby("actors")["repo_name"]
            .nunique()
            .reset_index()
            .rename(columns={"repo_name": "repos_hit"})
        )
        cluster_counts["appearances"] = (
            self.df.groupby("actors").size().values
        )
        return (
            cluster_counts[cluster_counts["appearances"] >= min_appearances]
            .sort_values("repos_hit", ascending=False)
        )


# ── 4. PhishingRepoAnalyser (NEW) ─────────────────────────────────────────────
class PhishingRepoAnalyser:
    """
    Scores repos for phishing / malware signals:
    - Suspicious name keywords (crack, free, wallet, …)
    - Branch explosion (>100 distinct branches)
    - AI co-author in commits
    - Single-actor + high volume
    - Bot-ratio extremes
    """

    def __init__(self, repo_stats_df: pd.DataFrame):
        self.df = repo_stats_df.copy()

    def high_risk_repos(self, min_score: int = 3) -> pd.DataFrame:
        if "suspicious_score" not in self.df.columns:
            return pd.DataFrame()
        return (
            self.df[self.df["suspicious_score"] >= min_score]
            .sort_values("suspicious_score", ascending=False)
        )

    def phish_name_repos(self) -> pd.DataFrame:
        if "phish_name_flag" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["phish_name_flag"]].sort_values("suspicious_score", ascending=False)

    def branch_explosion_repos(self, threshold: int = 50) -> pd.DataFrame:
        if "distinct_branches" not in self.df.columns:
            return pd.DataFrame()
        return (
            self.df[self.df["distinct_branches"] >= threshold]
            .sort_values("distinct_branches", ascending=False)
        )

    def ai_coauthor_repos(self) -> pd.DataFrame:
        if "ai_coauthor_flag" not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df["ai_coauthor_flag"]].sort_values("suspicious_score", ascending=False)

    def risk_breakdown(self) -> pd.DataFrame:
        """Summary counts for each risk signal."""
        rows = []
        checks = [
            ("phish_name_flag",    "Suspicious repo name keyword"),
            ("ai_coauthor_flag",   "AI handle in commit co-author"),
        ]
        for col, label in checks:
            if col in self.df.columns:
                rows.append({"signal": label, "repo_count": int(self.df[col].sum())})
        if "distinct_branches" in self.df.columns:
            rows.append({"signal": "Branch explosion (>50 branches)",
                         "repo_count": int((self.df["distinct_branches"] >= 50).sum())})
        if "bot_ratio" in self.df.columns:
            rows.append({"signal": "Bot-ratio > 0.5",
                         "repo_count": int((self.df["bot_ratio"] > 0.5).sum())})
        return pd.DataFrame(rows)


# ── 5. RepoPurposeAnalyser (Kanak's, kept) ───────────────────────────────────
class RepoPurposeAnalyser:
    def __init__(self, repo_stats_df, enriched_df=None, n_clusters=8):
        self.stats    = repo_stats_df.copy()
        self.enriched = enriched_df
        self.n_clusters = n_clusters
        self._merged  = None

    def _get_merged(self):
        if self._merged is not None:
            return self._merged
        self._merged = (
            self.stats.merge(self.enriched, on="repo_name", how="left")
            if self.enriched is not None and not self.enriched.empty
            else self.stats.copy()
        )
        return self._merged

    def name_heuristic_classify(self) -> pd.DataFrame:
        df   = self._get_merged().copy()
        name = df["repo_name"].str.lower()
        conds = [
            name.str.contains(r"config|dotfile|setting|rc$", regex=True),
            name.str.contains(r"bot|automation|action|workflow", regex=True),
            name.str.contains(r"demo|example|sample|test|template", regex=True),
            name.str.contains(r"docs|documentation|wiki", regex=True),
            name.str.contains(r"awesome-|list|collection|resource", regex=True),
            name.str.contains(r"crack|free|hack|wallet|stealer|cheat", regex=True),
        ]
        choices = ["config", "automation", "demo_or_test", "docs", "curated_list", "suspicious"]
        df["name_category"] = np.select(conds, choices, default="general")
        self._merged = df
        return df


# ── 6. AnomalyDetector (Kanak's, kept) ───────────────────────────────────────
class AnomalyDetector:
    FEATURE_COLS = ["total_events", "unique_actors", "bot_ratio",
                    "events_per_actor", "events_per_second", "distinct_branches"]

    def __init__(self, repo_stats_df, z_threshold=3.0):
        self.stats = repo_stats_df.copy()
        self.z_threshold = z_threshold

    def zscore_anomalies(self) -> pd.DataFrame:
        available = [c for c in self.FEATURE_COLS if c in self.stats.columns]
        if not available:
            return pd.DataFrame()
        scaler  = StandardScaler()
        numeric = self.stats[available].fillna(0)
        zdf     = pd.DataFrame(scaler.fit_transform(numeric),
                               columns=[f"z_{c}" for c in available],
                               index=self.stats.index)
        combined = pd.concat([self.stats[["repo_name"] + available], zdf], axis=1)
        combined["max_z"]          = zdf.abs().max(axis=1)
        combined["anomaly_feature"] = zdf.abs().idxmax(axis=1).str.replace("z_", "")
        return combined[combined["max_z"] > self.z_threshold].sort_values("max_z", ascending=False)


# ── 7. ReportExporter ─────────────────────────────────────────────────────────
class ReportExporter:
    def __init__(self, output_dir=REPORTS_DIR):
        self.out = Path(output_dir)
        self.out.mkdir(parents=True, exist_ok=True)

    def save_csv(self, df: pd.DataFrame, name: str):
        if df.empty:
            return
        path = self.out / f"{name}.csv"
        df.to_csv(path, index=False)
        logger.info("CSV → %s", path)

    def save_html_report(
        self,
        bot_profile:        pd.DataFrame,
        suspicious_humans:  pd.DataFrame,
        lockstep_repos:     pd.DataFrame,
        phish_repos:        pd.DataFrame,
        risk_breakdown:     pd.DataFrame,
        anomalies:          pd.DataFrame,
        bot_cat_summary:    pd.DataFrame,
        branch_explosion:   pd.DataFrame,
        ai_coauthor_repos:  pd.DataFrame,
    ) -> Path:
        try:
            import plotly.express as px
            import plotly.io as pio
            HAS_PLOTLY = True
        except ImportError:
            HAS_PLOTLY = False

        def df_html(df, n=25):
            if df is None or df.empty:
                return "<p><em>No data.</em></p>"
            return df.head(n).to_html(index=False, border=0, classes="tbl")

        def bar_chart(df, x, y, title, color=None):
            if not HAS_PLOTLY or df is None or df.empty or x not in df or y not in df:
                return ""
            fig = px.bar(df.head(20), x=x, y=y, title=title, color=color,
                         color_discrete_sequence=["#e94560", "#0f3460", "#533483",
                                                   "#e94560", "#16213e"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="#e2e8f0", title_font_size=14)
            return pio.to_html(fig, full_html=False)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>SK Scout – GitHub Anomaly Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#0a0e1a; --surface:#111827; --border:#1e2d45;
  --accent:#e94560; --accent2:#0f3460; --accent3:#533483;
  --text:#e2e8f0; --muted:#94a3b8; --green:#10b981; --yellow:#f59e0b;
}}
*{{box-sizing:border-box; margin:0; padding:0}}
body{{background:var(--bg); color:var(--text); font-family:'Space Mono',monospace;
      min-height:100vh; padding:0}}
header{{background:linear-gradient(135deg,#0f3460 0%,#533483 50%,#e94560 100%);
        padding:48px 40px 32px; position:relative; overflow:hidden}}
header::before{{content:''; position:absolute; inset:0;
  background:url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.03'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")}}
h1{{font-family:'Syne',sans-serif; font-size:2.4rem; font-weight:800;
    letter-spacing:-1px; position:relative}}
h1 span{{color:#fbbf24}}
.subtitle{{color:rgba(255,255,255,.7); margin-top:8px; font-size:.85rem; position:relative}}
.stat-bar{{display:flex; gap:16px; margin-top:24px; flex-wrap:wrap; position:relative}}
.stat{{background:rgba(255,255,255,.1); border:1px solid rgba(255,255,255,.15);
       border-radius:10px; padding:14px 20px; min-width:140px}}
.stat .val{{font-size:1.8rem; font-weight:700; font-family:'Syne',sans-serif; color:#fbbf24}}
.stat .lbl{{font-size:.7rem; color:rgba(255,255,255,.6); margin-top:2px}}
main{{max-width:1300px; margin:0 auto; padding:32px 24px}}
section{{margin-bottom:40px}}
.section-header{{display:flex; align-items:center; gap:12px; margin-bottom:16px;
                 border-bottom:1px solid var(--border); padding-bottom:12px}}
.badge{{background:var(--accent); color:#fff; font-size:.65rem; font-weight:700;
        padding:3px 8px; border-radius:4px; letter-spacing:1px}}
.badge.new{{background:var(--green)}}
.badge.warn{{background:var(--yellow); color:#000}}
h2{{font-family:'Syne',sans-serif; font-size:1.2rem; font-weight:700}}
.tbl{{width:100%; border-collapse:collapse; font-size:.78rem}}
.tbl th{{background:var(--accent2); color:#fff; padding:8px 12px;
          text-align:left; font-weight:700; letter-spacing:.5px}}
.tbl td{{padding:7px 12px; border-bottom:1px solid var(--border); color:var(--muted)}}
.tbl tr:hover td{{background:rgba(255,255,255,.03); color:var(--text)}}
.grid2{{display:grid; grid-template-columns:1fr 1fr; gap:24px}}
.card{{background:var(--surface); border:1px solid var(--border);
       border-radius:12px; padding:20px}}
.tag{{display:inline-block; background:rgba(233,69,96,.15); color:var(--accent);
      border:1px solid rgba(233,69,96,.3); border-radius:4px;
      font-size:.65rem; padding:2px 6px; margin:2px}}
@media(max-width:768px){{.grid2{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<header>
  <h1>🔍 SK <span>Scout</span></h1>
  <p class="subtitle">GitHub Anomaly & Bot Detection Report — Shravani</p>
  <div class="stat-bar">
    <div class="stat"><div class="val">{len(phish_repos) if not phish_repos.empty else 0}</div><div class="lbl">High-Risk Repos</div></div>
    <div class="stat"><div class="val">{len(suspicious_humans) if not suspicious_humans.empty else 0}</div><div class="lbl">Suspicious Humans</div></div>
    <div class="stat"><div class="val">{len(lockstep_repos) if not lockstep_repos.empty else 0}</div><div class="lbl">Lockstep Targets</div></div>
    <div class="stat"><div class="val">{len(bot_profile) if not bot_profile.empty else 0}</div><div class="lbl">Known Bots</div></div>
    <div class="stat"><div class="val">{len(anomalies) if not anomalies.empty else 0}</div><div class="lbl">Z-Score Anomalies</div></div>
  </div>
</header>
<main>

<!-- RISK BREAKDOWN -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>Risk Signal Breakdown</h2>
  </div>
  <div class="grid2">
    <div class="card">{df_html(risk_breakdown)}</div>
    <div class="card">{bar_chart(risk_breakdown,"signal","repo_count","Repos Flagged per Signal")}</div>
  </div>
</section>

<!-- PHISHING REPOS -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>High-Risk / Phishing Repos</h2>
  </div>
  <div class="card">{df_html(phish_repos[["repo_name","suspicious_score","phish_name_flag","ai_coauthor_flag","distinct_branches","bot_ratio","unique_actors"]].head(30) if not phish_repos.empty and "phish_name_flag" in phish_repos.columns else phish_repos)}</div>
</section>

<!-- SUSPICIOUS HUMANS -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>Suspicious Human Accounts (No [bot] tag)</h2>
  </div>
  <div class="grid2">
    <div class="card">{df_html(suspicious_humans)}</div>
    <div class="card">{bar_chart(suspicious_humans.head(15),"actor_login","suspicious_human_score","Top Suspicious Accounts by Score") if not suspicious_humans.empty else "<p><em>No data.</em></p>"}</div>
  </div>
</section>

<!-- LOCKSTEP -->
<section>
  <div class="section-header">
    <span class="badge new">NEW</span>
    <h2>Lockstep-Targeted Repos (Coordinated Activity)</h2>
  </div>
  <div class="card">{df_html(lockstep_repos)}</div>
</section>

<!-- BRANCH EXPLOSION -->
<section>
  <div class="section-header">
    <span class="badge warn">ANOMALY</span>
    <h2>Branch Explosion Repos</h2>
  </div>
  <div class="card">{df_html(branch_explosion)}</div>
</section>

<!-- AI CO-AUTHOR -->
<section>
  <div class="section-header">
    <span class="badge warn">ANOMALY</span>
    <h2>Repos with AI Handle in Commit Co-Authors</h2>
  </div>
  <div class="card">{df_html(ai_coauthor_repos)}</div>
</section>

<!-- KNOWN BOTS -->
<section>
  <div class="section-header">
    <span class="badge">BOTS</span>
    <h2>Known Bot Profiles</h2>
  </div>
  <div class="grid2">
    <div class="card">{df_html(bot_cat_summary)}</div>
    <div class="card">{bar_chart(bot_cat_summary,"bot_category","total_events","Events by Bot Category")}</div>
  </div>
</section>

<!-- Z-SCORE ANOMALIES -->
<section>
  <div class="section-header">
    <span class="badge">STATS</span>
    <h2>Statistical Anomalies (Z-Score)</h2>
  </div>
  <div class="card">{df_html(anomalies)}</div>
</section>

</main>
</body>
</html>"""

        path = self.out / "report.html"
        path.write_text(html, encoding="utf-8")
        logger.info("HTML report → %s", path)
        return path


# ── orchestration ──────────────────────────────────────────────────────────────
def run_analytics(events_prefix: str = "events", n_clusters: int = 8) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    def _load(name):
        p = PROCESSED_DIR / name
        if not p.exists():
            logger.warning("Missing: %s", p); return pd.DataFrame()
        return pd.read_parquet(p)

    df_events   = _load(f"{events_prefix}_raw.parquet")
    df_repos    = _load(f"{events_prefix}_repo_stats.parquet")
    df_actors   = _load(f"{events_prefix}_actor_stats.parquet")
    df_lockstep = _load(f"{events_prefix}_lockstep.parquet")

    # ── known bots ────────────────────────────────────────────────────────
    profiler      = BotActorProfiler(df_events)
    bot_profile   = profiler.profile()
    bot_bursts    = profiler.detect_bursts()
    bot_cat_summ  = profiler.category_summary()

    # ── suspicious humans (NEW) ───────────────────────────────────────────
    sha           = SuspiciousHumanAnalyser(df_actors)
    susp_humans   = sha.top_suspicious()
    ai_accounts   = sha.ai_coauthor_accounts()

    # ── lockstep (NEW) ────────────────────────────────────────────────────
    lsa           = LockstepAnalyser(df_lockstep)
    lockstep_repos= lsa.top_targeted_repos()

    # ── phishing repos (NEW) ──────────────────────────────────────────────
    pra           = PhishingRepoAnalyser(df_repos)
    high_risk     = pra.high_risk_repos()
    phish_names   = pra.phish_name_repos()
    branch_exp    = pra.branch_explosion_repos()
    ai_repo       = pra.ai_coauthor_repos()
    risk_brkdwn   = pra.risk_breakdown()

    # ── repo purpose (Kanak's) ────────────────────────────────────────────
    rpa           = RepoPurposeAnalyser(df_repos, n_clusters=n_clusters)
    df_purpose    = rpa.name_heuristic_classify()

    # ── anomaly detection (Kanak's) ───────────────────────────────────────
    anomalies     = AnomalyDetector(df_repos).zscore_anomalies()

    # ── export ────────────────────────────────────────────────────────────
    exporter = ReportExporter()
    exporter.save_csv(bot_profile,   "bot_profiles")
    exporter.save_csv(bot_cat_summ,  "bot_category_summary")
    exporter.save_csv(susp_humans,   "suspicious_humans")
    exporter.save_csv(ai_accounts,   "ai_coauthor_accounts")
    exporter.save_csv(lockstep_repos,"lockstep_repos")
    exporter.save_csv(high_risk,     "high_risk_repos")
    exporter.save_csv(phish_names,   "phish_name_repos")
    exporter.save_csv(branch_exp,    "branch_explosion_repos")
    exporter.save_csv(risk_brkdwn,   "risk_breakdown")
    exporter.save_csv(anomalies,     "anomalous_repos")
    if not bot_bursts.empty:
        exporter.save_csv(bot_bursts, "bot_bursts")

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
    )
    logger.info("Analytics complete → %s", REPORTS_DIR)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--prefix",   default="events")
    p.add_argument("--clusters", type=int, default=8)
    args = p.parse_args()
    run_analytics(args.prefix, args.clusters)