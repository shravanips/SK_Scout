import os
from dotenv import load_dotenv

load_dotenv()

def get_x_headers():
    bearer_token = os.getenv("X_BEARER_TOKEN")
    if not bearer_token:
        raise ValueError("X_BEARER_TOKEN not found in .env")

    return {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }