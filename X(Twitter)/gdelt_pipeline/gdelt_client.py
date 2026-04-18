import time
import requests


def fetch_gdelt_articles(
    query: str,
    max_records: int = 10,
    mode: str = "artlist",
    max_retries: int = 5,
    base_sleep: float = 5.0,
) -> dict:
    """
    Fetch article data from the GDELT DOC API with retry + backoff for 429s.
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": mode,
        "maxrecords": max_records,
        "format": "json",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/126.0 Safari/537.36"
    }

    last_error = None

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    sleep_time = float(retry_after)
                else:
                    sleep_time = base_sleep * (2 ** attempt)

                print(
                    f"GDELT rate-limited query='{query}'. "
                    f"Attempt {attempt + 1}/{max_retries}. Sleeping {sleep_time:.1f}s..."
                )
                time.sleep(sleep_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as exc:
            last_error = exc
            if attempt < max_retries - 1:
                sleep_time = base_sleep * (2 ** attempt)
                print(
                    f"Request failed for query='{query}'. "
                    f"Attempt {attempt + 1}/{max_retries}. Sleeping {sleep_time:.1f}s..."
                )
                time.sleep(sleep_time)
            else:
                break

    raise last_error if last_error else RuntimeError("Unknown GDELT request failure.")