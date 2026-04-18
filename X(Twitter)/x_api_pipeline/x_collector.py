import os
import requests
from dotenv import load_dotenv
from x_api_pipeline.x_auth import get_x_headers

load_dotenv()

def fetch_recent_posts(query="Hollywood", max_results=10):
    base_url = os.getenv("X_API_BASE_URL", "https://api.x.com/2")
    url = f"{base_url}/tweets/search/recent"

    params = {
        "query": query,
        "max_results": max_results,
        "tweet.fields": "created_at,public_metrics,lang,author_id"
    }

    response = requests.get(url, headers=get_x_headers(), params=params, timeout=30)
    response.raise_for_status()
    return response.json()