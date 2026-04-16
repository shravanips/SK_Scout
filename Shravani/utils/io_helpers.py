import json
from pathlib import Path

import pandas as pd


def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def save_json(data, filepath):
    ensure_dir(Path(filepath).parent)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_csv(records, filepath):
    ensure_dir(Path(filepath).parent)
    df = pd.DataFrame(records)
    df.to_csv(filepath, index=False, encoding="utf-8")