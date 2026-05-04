# GitGub 🔍

**GitHub bot-actor & repository anomaly detection pipeline**  
Built on [GHArchive](https://www.gharchive.org/) — a record of every public GitHub event.

---

## What it does

| Step | Module | Description |
|------|--------|-------------|
| **Ingest** | `src/ingest.py` | Downloads GHArchive `.json.gz` files, streams & parses events, flags known bots AND suspicious humans, extracts phishing/branch/AI-coauthor signals, saves 4 Parquet tables |
| **Analytics** | `src/analytics.py` | 8 analysis classes — bot profiling, suspicious human detection, lockstep analysis, phishing repo scoring, purpose clustering, correlation, anomaly detection, HTML report |
| **Pipeline** | `run_pipeline.py` | One-command wrapper with per-run isolated output folders |

---

## Folder structure

```
gitgub/
├── run_pipeline.py          # entry point
├── requirements.txt
├── README.md
├── Makefile                 # common-command shortcuts
├── .env.example             # copy to .env
│
├── src/
│   ├── config.py            # ★ all constants, paths, thresholds (centralised)
│   ├── ingest.py            # download + parse + all signal extraction
│   ├── analytics.py         # 8 analysis classes + HTML report
│   └── utils.py             # logging, Parquet I/O, GitHub API helpers
│
├── notebooks/
│   └── exploration.ipynb    # original notebook (kept for reference)
│
├── data/
│   ├── raw/
│   │   └── gharchive/       # downloaded .json.gz files (git-ignored)
│   ├── processed/           # flat layout Parquet files (git-ignored)
│   ├── runs/                # ★ per-run isolated folders (Shravani)
│   │   └── <run_name>/
│   │       ├── processed/   #   Parquet for this run
│   │       ├── reports/     #   CSV + HTML for this run
│   │       ├── logs/        #   run.log
│   │       └── run_info.json
│   └── reports/             # flat-layout reports (standalone analytics)
│
├── tests/
│   ├── conftest.py          # shared fixtures
│   ├── test_ingest.py
│   ├── test_analytics.py
│   └── test_utils.py
│
└── logs/
    └── run.log
```

---

## Quick start

### 1. Clone & install

```bash
git clone <your-repo-url>
cd gitgub
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set up GitHub token (recommended)

Raises API rate limit from 60 → 5 000 req/hr.

```bash
cp .env.example .env
# edit .env and paste your token
export $(grep -v '^#' .env | xargs)
```

### 3. Run

```bash
# Fastest — 1 hour, 5k events, no API calls
make run-sanity

# 4-hour window with GitHub enrichment
make run-4h

# Full day
make run-day

# Full week
make run-week

# Or manually with custom dates
python run_pipeline.py \
  --run-tag myrun \
  --start "2026-04-15 10" \
  --end   "2026-04-15 14" \
  --no-enrich
```

### 4. View the report

Each run saves its report inside its own folder:
```
data/runs/<run_name>/reports/report.html
```

---

## Signal inventory

### Ingest signals (per event)

| Signal | Source | Description |
|--------|--------|-------------|
| `is_bot_actor` | Kanak | Matches 19 known bot login patterns |
| `phish_name` | Shravani | Repo name contains phishing keyword (crack, stealer, wallet, …) |
| `refs` | Shravani | Branch/tag ref names from CreateEvent payloads |
| `ai_coauthor` | Shravani | AI handle (Claude, Copilot, GPT-4, …) in commit co-author |

### Repo-level stats

| Column | Source | Description |
|--------|--------|-------------|
| `bot_ratio` | Kanak | Fraction of events from known bots |
| `events_per_actor` | Kanak | Volume concentration |
| `events_per_second` | Kanak | Activity velocity |
| `event_type_diversity` | Kanak | Distinct event types |
| `phish_name_flag` | Shravani | Any event had a phishing repo name |
| `ai_coauthor_flag` | Shravani | Any commit co-authored by AI |
| `distinct_branches` | Shravani | Unique branch refs in CreateEvents |
| `suspicious_score` | Both | Composite score (Kanak base + Shravani weights) |

### Actor-level stats (Shravani)

| Column | Description |
|--------|-------------|
| `event_entropy` | Shannon entropy of event types (low = robotic) |
| `burst_fraction` | Fraction of inter-event gaps < 60 s |
| `suspicious_human_score` | 0–5 score for non-bot accounts |

---

## Analytics classes

| Class | Source | Description |
|-------|--------|-------------|
| `BotActorProfiler` | Kanak | Known-bot profiling, burst detection, category summary |
| `SuspiciousHumanAnalyser` | Shravani | Non-bot accounts behaving like bots |
| `RepoPurposeAnalyser` | Kanak | TF-IDF + KMeans clustering, name heuristics |
| `BotRepoCorrelation` | Kanak | Cross-tab: bot category × repo purpose |
| `LockstepAnalyser` | Shravani | Coordinated multi-account activity windows |
| `PhishingRepoAnalyser` | Shravani | Name patterns, branch explosion, AI co-authors |
| `AnomalyDetector` | Kanak + Shravani | Z-score outliers (Shravani added `distinct_branches`) |
| `ReportExporter` | Both | Shravani's dark-theme HTML design + Kanak's correlation section |

---

## CSV outputs

| File | Source |
|------|--------|
| `bot_profiles.csv` | Kanak |
| `bot_category_summary.csv` | Kanak |
| `bot_heavy_repos.csv` | Kanak |
| `bot_repo_correlation.csv` | Kanak |
| `top_repos_per_bot.csv` | Kanak |
| `anomalous_repos.csv` | Kanak |
| `bot_bursts.csv` | Kanak |
| `suspicious_humans.csv` | Shravani |
| `ai_coauthor_accounts.csv` | Shravani |
| `lockstep_repos.csv` | Shravani |
| `high_risk_repos.csv` | Shravani |
| `phish_name_repos.csv` | Shravani |
| `branch_explosion_repos.csv` | Shravani |
| `ai_coauthor_repos.csv` | Shravani |
| `risk_breakdown.csv` | Shravani |

---

## Running tests

```bash
make test          # full suite
make test-cov      # with HTML coverage report
make test-ingest   # ingest only
make test-analytics
make test-utils
```

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_TOKEN` | `""` | Personal access token (5 000 req/hr with; 60 without) |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `DATA_DIR` | `data/` | Override data directory root |

---

## Suggested next additions

- **NLP on README content** — fetch README via GitHub API and run topic modelling
- **Time-series Streamlit dashboard** — drill-down bot activity over time
- **Bot network graph** — bipartite graph (bots ↔ repos) with NetworkX community detection
- **Diff between runs** — compare two `data/runs/` folders to spot newly-active bots
- **Alerting** — Slack/email when a repo's suspicious_score crosses a threshold

---

## License

MIT
