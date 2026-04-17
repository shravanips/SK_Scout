# GitGub 🤖

**GitHub bot-actor & repository analysis pipeline**  
Built on top of [GHArchive](https://www.gharchive.org/) — a record of every public GitHub event.

---

## What it does

| Step | Module | Description |
|------|--------|-------------|
| **Ingest** | `src/ingest.py` | Downloads GHArchive `.json.gz` files, streams & parses events, flags bot actors, computes per-repo statistics, saves to Parquet |
| **Analytics** | `src/analytics.py` | Bot profiling, repo purpose clustering, bot↔repo correlation, anomaly detection, HTML report |
| **Pipeline** | `run_pipeline.py` | One-command wrapper for both steps |

---

## Folder structure

```
gitgub/
├── run_pipeline.py          # entry point
├── requirements.txt
├── README.md
├── .env.example             # copy to .env and add your GitHub token
│
├── src/
│   ├── ingest.py            # download + parse + bot detection
│   ├── analytics.py         # bot profiler, repo clusterer, anomaly detector
│   └── utils.py             # logging, Parquet I/O, GitHub API helpers
│
├── notebooks/
│   └── exploration.ipynb    # original notebook (kept for reference)
│
├── data/
│   ├── raw/
│   │   └── gharchive/       # downloaded .json.gz files (git-ignored)
│   ├── processed/           # Parquet files (git-ignored)
│   └── reports/             # CSV + HTML outputs
│
├── tests/
│   └── test_ingest.py
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
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set up GitHub token (recommended)

A token gives you 5 000 API calls/hr instead of 60.

```bash
cp .env.example .env
# edit .env and paste your token
export $(cat .env)
```

### 3. Run

```bash
# Quick test — 1 hour of data, no GitHub API calls
python run_pipeline.py \
  --start "2026-04-15 12" \
  --end   "2026-04-15 13" \
  --max-events 10000 \
  --no-enrich

# Full run — 4 hours, with GitHub enrichment
python run_pipeline.py \
  --start "2026-04-15 10" \
  --end   "2026-04-15 14"

# Large run — 24 hours
python run_pipeline.py \
  --start "2026-04-15 00" \
  --end   "2026-04-16 00" \
  --max-enrich 500
```

### 4. View the report

Open `data/reports/report.html` in your browser.

---

## Running modules independently

```bash
# Ingest only
python src/ingest.py --start "2026-04-15 12" --end "2026-04-15 14"

# Analytics only (requires processed Parquet files)
python src/analytics.py --no-enrich --clusters 6
```

---

## Local setup changes vs original notebook

The original notebook had several hard-coded paths and ran on a single 1 000-event
sample. Here is what changed to make it run on your local machine:

| Original notebook | This project |
|---|---|
| Hard-coded absolute path `/Users/shravanisawant/…` | Paths relative to project root; raw files auto-downloaded |
| Single `.json.gz` file, 1 000-event cap | Configurable date/hour range; unlimited or capped via `--max-events` |
| All code in one notebook | `ingest.py` / `analytics.py` / `utils.py` separation |
| No caching | Downloaded files cached in `data/raw/`; Parquet in `data/processed/` |
| Bot detection: `[bot]` suffix only | Extended regex covers 10+ known bot name patterns |
| No GitHub API calls | Optional enrichment with language, topics, description |
| No output files | CSV + HTML report in `data/reports/` |

---

## What makes this unique

Beyond the original notebook, this project adds:

1. **Bot taxonomy** — classifies bots into functional categories (dependency management, CI/CD, security, docs, translation). See `classify_bot()` in `analytics.py`.

2. **Repo purpose clustering** — TF-IDF + KMeans on repository descriptions groups repos into topics automatically. Combine with GitHub topics for a multi-signal picture.

3. **Bot × purpose correlation** — cross-tab of bot categories vs repo clusters reveals that e.g. `dependabot` concentrates in library repos while `github-actions` bots appear uniformly.

4. **Temporal burst detection** — sliding window counts flag bot accounts that fire 20+ events in a 10-minute window (coordination or webhook spam signal).

5. **Cross-repo footprint** — bots active in 5+ repos are flagged; high-footprint bots are different from single-repo automation.

6. **Z-score anomaly detection** — statistical outliers on `bot_ratio`, `events_per_second`, etc. surface the most unusual repos without manual threshold-tuning.

7. **Parallel downloads** — `ThreadPoolExecutor` fetches multiple hours concurrently, making 24-hour runs practical.

8. **Self-contained HTML report** — embedded Plotly charts, no external dependencies needed to view.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GITHUB_TOKEN` | `""` | Personal access token for GitHub API (5 000 req/hr with; 60 without) |

---

## Suggested next additions

- **NLP on README content** — fetch `README.md` via GitHub API and run topic modelling (LDA / BERTopic) for richer repo purpose labels.
- **Time-series dashboard** — Streamlit app showing bot activity over time with drill-down by category.
- **Bot network graph** — build a bipartite graph (bots ↔ repos) and detect communities with NetworkX.
- **Diff with previous run** — compare two time windows to spot newly-active bots.
- **Alerting** — send a Slack/email notification when a repo's suspicion score crosses a threshold.

---

## License

MIT
