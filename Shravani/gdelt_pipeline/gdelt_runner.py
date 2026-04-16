import time
from pathlib import Path

from config.keywords import TOPIC_BUCKETS
from config.settings import RAW_GDELT_DIR, CLEANED_GDELT_DIR
from gdelt_pipeline.gdelt_client import fetch_gdelt_articles
from gdelt_pipeline.gdelt_parser import parse_gdelt_articles
from utils.io_helpers import save_json, save_csv


def run_gdelt_collection():
    print("Running GDELT pipeline...")

    Path(RAW_GDELT_DIR).mkdir(parents=True, exist_ok=True)
    Path(CLEANED_GDELT_DIR).mkdir(parents=True, exist_ok=True)

    for topic, keywords in TOPIC_BUCKETS.items():
        for keyword in keywords:
            print(f"Fetching GDELT data for topic='{topic}' keyword='{keyword}'")

            try:
                raw_data = fetch_gdelt_articles(query=keyword, max_records=5)
                parsed_rows = parse_gdelt_articles(raw_data, topic=topic, keyword=keyword)

                safe_name = keyword.lower().replace(" ", "_")
                raw_file = Path(RAW_GDELT_DIR) / f"gdelt_{topic}_{safe_name}.json"
                cleaned_file = Path(CLEANED_GDELT_DIR) / f"gdelt_{topic}_{safe_name}.csv"

                save_json(raw_data, raw_file)
                save_csv(parsed_rows, cleaned_file)

                print(f"Saved raw JSON: {raw_file}")
                print(f"Saved cleaned CSV: {cleaned_file}")
                print(f"Records saved: {len(parsed_rows)}")

            except Exception as exc:
                print(f"Error for topic='{topic}' keyword='{keyword}': {exc}")

            print("Sleeping 10 seconds before next keyword...")
            time.sleep(10)