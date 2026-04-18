def parse_x_posts(response_json, query):
    posts = response_json.get("data", [])
    parsed = []

    for post in posts:
        metrics = post.get("public_metrics", {})
        parsed.append({
            "query": query,
            "id": post.get("id"),
            "text": post.get("text"),
            "author_id": post.get("author_id"),
            "created_at": post.get("created_at"),
            "lang": post.get("lang"),
            "retweet_count": metrics.get("retweet_count"),
            "reply_count": metrics.get("reply_count"),
            "like_count": metrics.get("like_count"),
            "quote_count": metrics.get("quote_count"),
        })

    return parsed