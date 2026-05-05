# SK_Scout 🔍  
**A Multi-Level Suspicious Activity Detection Framework for GitHub Ecosystems**

Built on [GHArchive](https://www.gharchive.org/) — a large-scale public record of GitHub activity.

---

## 🧠 Overview

SK_Scout is a **behavior-driven analysis system** designed to detect **suspicious, automated, and coordinated activity** in large-scale software ecosystems.

Unlike traditional approaches that rely on **single indicators (e.g., bot labels or star counts)**, SK_Scout integrates:

- Actor-level behavioral signals  
- Repository-level anomalies  
- Temporal coordination patterns  
- Metadata and semantic indicators  

This enables **multi-dimensional detection** of:

- Bot-heavy repositories  
- Suspicious human-like behavior  
- Coordinated (lockstep) activity  
- Potential phishing or malicious repositories  

---

## 🎯 Research Motivation

Modern software ecosystems like GitHub are increasingly influenced by:

- Automated agents (bots)
- AI-assisted contributions
- Coordinated manipulation (e.g., fake engagement)

Existing detection approaches are often:

- Narrow (single-signal based)
- Static (no temporal awareness)
- Limited to known bot patterns

**SK_Scout addresses this gap** by introducing a **multi-stage analytical pipeline** that captures both **behavioral and structural anomalies at scale**.

---

## ⚙️ System Architecture

       GHArchive (.json.gz)
                │
                ▼
    ┌────────────────────┐
    │   Ingestion Layer  │
    │  (Event Parsing)   │
    └────────────────────┘
                │
                ▼
    ┌────────────────────┐
    │ Signal Extraction  │
    │ (Actor + Repo)     │
    └────────────────────┘
                │
                ▼
    ┌────────────────────┐
    │ Analytics Engine   │
    │ (8 Modules)        │
    └────────────────────┘
                │
                ▼
    ┌────────────────────┐
    │ Detection Outputs  │
    │ CSV + HTML Report  │
    └────────────────────┘


---

## 🚀 Key Contributions

- **Multi-level detection framework** combining actor, repository, and temporal signals  
- **Suspicious human detection** beyond known bot identification  
- **Lockstep analysis** for coordinated multi-account activity  
- **Phishing-aware repository scoring** using semantic + structural signals  
- **Scalable pipeline** capable of processing large GHArchive datasets  
- **Interactive reporting layer** for exploratory analysis  

---

## 🧩 Pipeline Components

| Stage | Module | Description |
|------|--------|-------------|
| **Ingestion** | `src/ingest.py` | Streams GHArchive events, extracts signals, builds structured datasets |
| **Analytics** | `src/analytics.py` | Multi-module analysis engine (behavioral, statistical, clustering) |
| **Pipeline Runner** | `run_pipeline.py` | End-to-end execution with isolated run tracking |

---

## 📊 Detection Signals

### 🔹 Actor-Level Signals
- Event entropy (behavioral diversity)
- Burst activity patterns
- Suspicious human scoring (0–5 scale)

### 🔹 Repository-Level Signals
- Bot activity ratio
- Event velocity (events/sec)
- Branch explosion patterns
- AI-assisted contributions
- Phishing keyword detection

### 🔹 Coordination Signals
- Lockstep activity windows across accounts
- Temporal clustering of actions

---

## 🧪 Analytical Modules

| Module | Description |
|--------|------------|
| `BotActorProfiler` | Categorizes and profiles known bot behaviors |
| `SuspiciousHumanAnalyser` | Identifies human accounts with bot-like patterns |
| `RepoPurposeAnalyser` | Clusters repositories using TF-IDF + KMeans |
| `BotRepoCorrelation` | Links bot categories to repository purposes |
| `LockstepAnalyser` | Detects coordinated multi-account activity |
| `PhishingRepoAnalyser` | Flags high-risk repositories using semantic signals |
| `AnomalyDetector` | Identifies statistical outliers via Z-score analysis |
| `ReportExporter` | Generates structured CSV + interactive HTML reports |

---

## 📁 Project Structure

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

## 📌 Example Use Cases

SK_Scout can be used in multiple research and practical scenarios:

- **Security analysis** — Identify suspicious or potentially malicious repositories  
- **Bot ecosystem study** — Analyze large-scale bot behavior across GitHub  
- **Platform integrity research** — Detect coordinated manipulation patterns  
- **AI-assisted development tracking** — Study trends in AI co-authored code  
- **Anomaly detection benchmarking** — Evaluate behavioral anomaly detection techniques  

---

## 📊 What to Look For in the Report

After running the pipeline, the generated report highlights:

- Repositories with **high bot activity concentration**  
- Accounts exhibiting **bot-like human behavior**  
- **Coordinated activity clusters** (lockstep patterns)  
- Repositories flagged for **phishing or suspicious naming patterns**  
- Statistical **outliers across multiple behavioral dimensions**  

This helps quickly identify high-risk entities without manual inspection.

---

## ⚡ Performance Notes

- Designed to scale with large GHArchive datasets  
- Supports both **lightweight runs (1–4 hours)** and **extended runs (day/week)**  
- Optional GitHub API enrichment improves context but is not required  
- Per-run isolation ensures reproducibility and clean experiment tracking  

---

## 🧪 Reproducibility

Each pipeline run is fully isolated and stored under:

data/runs/<run_name>/

Each run contains:

- Processed datasets  
- Logs  
- Structured CSV outputs  
- Final HTML report  
- Run metadata (`run_info.json`)  

This enables consistent comparison across different time windows and configurations.

---

## 🔍 Design Philosophy

SK_Scout follows a **multi-signal detection approach**:

- No single signal determines suspiciousness  
- Behavioral, structural, and temporal signals are combined  
- Detection is based on **patterns, not labels**  

This makes the system more robust to:

- Evolving bot strategies  
- Unknown attack patterns  
- Human-like automated behavior  

---

## License

MIT
