from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"

RAW_GDELT_DIR = RAW_DIR / "gdelt"
CLEANED_GDELT_DIR = CLEANED_DIR / "gdelt"