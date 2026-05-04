"""
config.py
"""

import os
import re
from pathlib import Path

# ── project root 
ROOT = Path(__file__).parent.parent.resolve()


# ── directory layout 
class PATHS:
    ROOT      = ROOT
    SRC       = ROOT / "src"
    DATA      = ROOT / "data"
    RAW       = ROOT / "data" / "raw" / "gharchive"
    PROCESSED = ROOT / "data" / "processed"   # flat/legacy layout 
    RUNS      = ROOT / "data" / "runs"        # per-run isolated layout 
    REPORTS   = ROOT / "data" / "reports"
    LOGS      = ROOT / "logs"
    NOTEBOOKS = ROOT / "notebooks"
    TESTS     = ROOT / "tests"

    @classmethod
    def run_dir(cls, run_name: str) -> Path:
        """Return the root directory for a named pipeline run."""
        return cls.RUNS / run_name

    @classmethod
    def all_required(cls) -> list[Path]:
        """Directories that must exist before the pipeline starts."""
        return [cls.RAW, cls.PROCESSED, cls.RUNS, cls.REPORTS, cls.LOGS]


# ── GHArchive 
GHARCHIVE_URL_TEMPLATE = (
    "https://data.gharchive.org/"
    "{year}-{month:02d}-{day:02d}-{hour}.json.gz"
)

# ── known-bot detection
# Add new patterns HERE.

BOT_PATTERNS: list[str] = [
    r"\[bot\]",
    r"-bot$",
    r"^bot-",
    r"dependabot",
    r"renovate",
    r"github-actions",
    r"codecov",
    r"snyk-bot",
    r"greenkeeper",
    r"semantic-release",
    r"allcontributors",
    r"imgbot",
    r"mend-bolt",
    r"whitesource",
    r"deepsource",
    r"codeclimate",
    r"crowdin",
    r"transifex",
    r"lokalise",
    r"travis",
]

BOT_RE = re.compile("|".join(BOT_PATTERNS), re.IGNORECASE)

# ── phishing / malware repo-name keywords 
PHISH_KEYWORDS: list[str] = [
    "crack", "cracked", "free", "hack", "cheat", "stealer", "wallet",
    "crypto", "bot", "autoclicker", "executor", "solana", "roblox",
    "adobe", "activation", "keygen", "nulled", "leaked", "bypass",
    "spoofer", "rat", "trojan", "grabber", "logger",
]

PHISH_RE = re.compile("|".join(PHISH_KEYWORDS), re.IGNORECASE)

# ── AI handle co-author detection 
AI_HANDLES_RE = re.compile(
    r"claude|copilot|chatgpt|openai|gpt-?4|gemini", re.IGNORECASE
)

# ── bot taxonomy 
BOT_TAXONOMY: dict[str, list[str]] = {
    "dependency":   ["dependabot", "renovate", "greenkeeper", "depfu"],
    "ci_cd":        ["github-actions", "travis", "circleci", "semantic-release"],
    "security":     ["snyk-bot", "mend-bolt", "whitesource"],
    "code_quality": ["codecov", "codeclimate", "deepsource"],
    "docs":         ["allcontributors", "imgbot", "readme-bot"],
    "translation":  ["lokalise", "crowdin", "transifex"],
}

# ── repo name heuristics 

REPO_NAME_CATEGORIES: dict[str, str] = {
    r"config|dotfile|setting|rc$":           "config",
    r"bot|automation|action|workflow":        "automation",
    r"demo|example|sample|test|template":    "demo_or_test",
    r"docs|documentation|wiki":             "docs",
    r"awesome-|list|collection|resource":   "curated_list",
    r"crack|free|hack|wallet|stealer|cheat": "suspicious",   
}

# ── suspicion scoring weights 
class REPO_SUSPICION:
    EVENTS_PER_ACTOR_THRESHOLD  = 10
    BOT_RATIO_THRESHOLD         = 0.5
    UNIQUE_ACTORS_MIN           = 1
    EVENTS_PER_SECOND_THRESHOLD = 0.1
    PHISH_NAME_WEIGHT           = 3    
    AI_COAUTHOR_WEIGHT          = 2    
    BRANCH_EXPLOSION_THRESHOLD  = 100  
    BRANCH_EXPLOSION_WEIGHT     = 2    

# ── suspicious human scoring 
class ACTOR_SUSPICION:
    ENTROPY_THRESHOLD = 0.5   # below this → flag
    BURST_THRESHOLD   = 0.6   # fraction of inter-event gaps < 60 s
    HIGH_VOLUME       = 20    # events within window

# ── analytics defaults 
class DEFAULT_PARAMS:
    N_CLUSTERS              = 8
    MAX_ENRICH_REPOS        = 300
    MAX_WORKERS             = 4
    CHUNK_SIZE              = 8192
    BURST_WINDOW_MIN        = 10
    BURST_THRESHOLD         = 20
    WIDE_FOOTPRINT_REPOS    = 5
    BOT_HEAVY_THRESHOLD     = 0.5
    Z_SCORE_THRESHOLD       = 3.0
    API_DELAY_S             = 0.1
    API_RETRIES             = 3
    TFIDF_MAX_FEATURES      = 500
    TFIDF_MIN_DF            = 1   # 2 prunes everything on small corpora (< ~10 docs)
    LOCKSTEP_WINDOW_MIN     = 30   
    LOCKSTEP_MIN_ACCOUNTS   = 3    
    BRANCH_EXPLOSION_THRESH = 50   
    SUSP_HUMAN_MIN_SCORE    = 2    
    SUSP_HUMAN_TOP_N        = 50   

# ── environment / secrets 
GITHUB_TOKEN: str = os.environ.get("GITHUB_TOKEN", "")
LOG_LEVEL: str    = os.environ.get("LOG_LEVEL", "INFO")
DATA_DIR: Path    = Path(os.environ.get("DATA_DIR", str(PATHS.DATA)))

# ── datetime format 
DATETIME_FMT = "%Y-%m-%d %H"
