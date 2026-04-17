# X Bot Intelligence Pipeline

Scrapes celebrity profile data from X (Twitter), scores followers and 
comments for bot activity, and links engagement patterns to trending 
topic authenticity. Built to answer: **which trending topics are organic 
vs artificially hyped / paid?**

---

## Folder Structure

```
X/
├── pipeline.py              # Master orchestrator — run everything here
├── db.py                    # SQLite schema + connection helpers
├── utils.py                 # Shared helpers (config, logging, Tweepy factory)
│
├── scrape_profiles.py       # Stage 2: Celebrity profile data
├── scrape_posts.py          # Stage 3: All posts since Jan 2020
├── scrape_replies.py        # Stage 4: Comments on top posts
├── scrape_followers.py      # Stage 5: Follower sampling
├── scrape_trending.py       # Stage 6: Trending topic capture
│
├── bot_detector.py          # Stage 7: Multi-signal bot scoring
├── trending_analyzer.py     # Stage 8: Trend authenticity analysis
├── export_data.py           # Stage 9: CSV / Parquet exports
│
├── config/
│   └── config.yaml          # ← Put your API keys here
├── data/
│   ├── raw/                 # Raw API blobs (for debugging)
│   ├── processed/           # Cleaned CSVs / Parquet for modelling
│   └── trending/            # Per-trend tweet sample CSVs
├── logs/                    # Per-module log files
├── models/                  # Save your trained models here
└── reports/                 # Generated analysis reports
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Add your API keys

Edit `config/config.yaml`:

```yaml
api:
  bearer_token: "YOUR_BEARER_TOKEN"
  api_key: "YOUR_API_KEY"
  api_secret: "YOUR_API_SECRET"
  access_token: "YOUR_ACCESS_TOKEN"
  access_token_secret: "YOUR_ACCESS_TOKEN_SECRET"
```

Or use environment variables (recommended):

```bash
export X_BEARER_TOKEN="..."
export X_API_KEY="..."
export X_API_SECRET="..."
export X_ACCESS_TOKEN="..."
export X_ACCESS_TOKEN_SECRET="..."
```

### 3. API Tier Requirements

| Feature | Free | Basic | Pro / Academic |
|---|---|---|---|
| Profile data | ✅ | ✅ | ✅ |
| Recent tweets (7 days) | ✅ | ✅ | ✅ |
| **All tweets since 2020** | ❌ | ❌ | ✅ Required |
| Reply search | Limited | ✅ | ✅ |
| Follower lookup | ❌ | ✅ | ✅ |
| Trending topics | ❌ | ✅ | ✅ |

For the full historical dataset (2020–present), apply for 
[Academic Research Access](https://developer.twitter.com/en/products/twitter-api/academic-research).

---

## Running the Pipeline

### Full pipeline (all stages)

```bash
python pipeline.py
```

### Single celebrity

```bash
python pipeline.py --username KimKardashian
```

### Selective stages

```bash
# Only initialise DB and scrape profiles + posts
python pipeline.py --stages 1,2,3

# Skip follower sampling (slow) and trending (no API access)
python pipeline.py --skip 5,6
```

### Preview what will run

```bash
python pipeline.py --dry-run
python pipeline.py --stages 3,4,7 --username selenagomez --dry-run
```

### All options

```
--config            Path to config YAML [default: config/config.yaml]
--stages            Comma-separated stage numbers to run
--skip              Comma-separated stage numbers to skip
--username          Limit to a single celebrity
--posts-per-user    Posts to collect replies for in stage 4 [default: 50]
--replies-per-post  Max replies per post in stage 4 [default: 200]
--follower-sample-size  Followers to sample per celebrity [default: 1000]
--export-format     csv or parquet [default: csv]
--fail-fast         Stop on first failure
--dry-run           Print plan without executing
```

---

## Running Modules Individually

```bash
# Profiles
python scrape_profiles.py

# Posts for one celebrity
python scrape_posts.py --username taylorswift13

# Replies
python scrape_replies.py --username KimKardashian --posts-per-user 20

# Follower sampling
python scrape_followers.py --sample-size 2000

# Live trending (runs indefinitely, polls every hour)
python scrape_trending.py --mode live

# One-off trending snapshot
python scrape_trending.py --mode snapshot

# Historical trend reconstruction (needs Pro/Academic)
python scrape_trending.py --mode historical --trend "#GrammyAwards" --start-date 2020-01-01

# Bot detection — all profiles
python bot_detector.py

# Bot detection — one profile, one mode
python bot_detector.py --username KimKardashian --mode comments

# Trend authenticity analysis
python trending_analyzer.py
python trending_analyzer.py --trend "#Oscars2022"

# Export
python export_data.py
python export_data.py --format parquet
```

---

## Data Outputs (after export)

| File | Description |
|---|---|
| `processed/profiles.csv` | Celebrity profile snapshots |
| `processed/posts.csv` | All posts with full engagement metrics |
| `processed/replies_with_bot_scores.csv` | Comments + per-author bot score |
| `processed/follower_samples_with_bot_scores.csv` | Sampled followers + bot score |
| `processed/bot_analysis_summary.csv` | Per-celebrity bot verdict |
| `processed/trending_analysis.csv` | Trend authenticity verdicts |
| `processed/engagement_features.csv` | **ML-ready feature matrix** |
| `processed/follower_growth.csv` | Follower count over time |

---

## Bot Detection Signals

### Account-level signals (followers & commenters)

| Signal | Weight | Description |
|---|---|---|
| No profile picture | +0.30 | Strong bot indicator |
| No bio | +0.20 | Moderate bot indicator |
| Account age < 90 days | +0–0.30 | Scaled by how new |
| Low follower/following ratio | +0.25 | Bots follow many, have few |
| Zero tweets | +0.20 | Ghost account |
| Hyper-active (>100K tweets) | +0.15 | Bot factory |
| Verified account | −0.30 | Authenticity bonus |

### Engagement-level signals (per post)

| Signal | Description |
|---|---|
| Engagement rate > 80% | Unrealistically high — likely inflated |
| Engagement rate < 0.01% | Zombie engagement — bot audience |
| >5σ engagement spike | Coordinated push |
| Uniform engagement CV < 0.1 | Suspiciously flat across all posts |

### Comment-level signals

| Signal | Description |
|---|---|
| TF-IDF cosine similarity > 0.85 | Near-duplicate comments |
| >10 comments in 5-minute window | Coordinated timing burst |

---

## Trending Topic Verdicts

| Verdict | Authenticity Score | Meaning |
|---|---|---|
| `authentic` | > 0.75 | Organic engagement, low bot presence |
| `suspicious` | 0.45–0.75 | Mixed signals, needs investigation |
| `likely_paid` | < 0.45 | High bot concentration, coordinated push |

---

## Adding More Celebrities

Add entries to `config/config.yaml` under `targets:`:

```yaml
targets:
  actors:
    - username: "YourNewCeleb"
      name: "Their Full Name"
```

Then add their category to `CATEGORY_MAP` in `scrape_profiles.py`.

---

## Database Schema Overview

```
profiles              ← Celebrity profile data + snapshots
posts                 ← Every tweet since Jan 2020
replies               ← Comments with author metadata + bot score
follower_samples      ← Sampled followers + bot score
trending_topics       ← Raw trending snapshots (hourly)
trending_analysis     ← Authenticity verdicts per trend
bot_analysis          ← Per-celebrity bot summary
pipeline_runs         ← Audit log of every run
```

All data lives in `data/x_pipeline.db` (SQLite, portable, no server needed).
