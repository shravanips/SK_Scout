from x_api_pipeline.x_collector import fetch_recent_posts
from x_api_pipeline.x_parser import parse_x_posts
from utils.io_helpers import save_json, save_csv
from config.settings import RAW_X_DIR, CLEANED_X_DIR

def run_x_collection():
    print("Running X API pipeline...")

    query = "Hollywood"
    raw_data = fetch_recent_posts(query=query, max_results=10)
    parsed_data = parse_x_posts(raw_data, query=query)

    raw_file = RAW_X_DIR / "x_recent_hollywood.json"
    cleaned_file = CLEANED_X_DIR / "x_recent_hollywood.csv"

    save_json(raw_data, raw_file)
    save_csv(parsed_data, cleaned_file)

    print(f"Saved raw data to: {raw_file}")
    print(f"Saved cleaned data to: {cleaned_file}")