def parse_gdelt_articles(raw_data: dict, topic: str, keyword: str) -> list[dict]:
    """
    Convert raw GDELT response into a cleaner list of dictionaries.
    """
    articles = raw_data.get("articles", [])
    parsed_rows = []

    for article in articles:
        parsed_rows.append(
            {
                "topic_bucket": topic,
                "keyword": keyword,
                "title": article.get("title"),
                "url": article.get("url"),
                "domain": article.get("domain"),
                "seendate": article.get("seendate"),
                "language": article.get("language"),
                "sourcecountry": article.get("sourcecountry"),
                "socialimage": article.get("socialimage"),
            }
        )

    return parsed_rows


def parse_gdelt_articles(raw_data: dict, topic: str, keyword: str) -> list[dict]:
    articles = raw_data.get("articles", [])
    parsed_rows = []

    for article in articles:
        language = article.get("language")
        if language != "English":
            continue

        parsed_rows.append(
            {
                "topic_bucket": topic,
                "keyword": keyword,
                "title": article.get("title"),
                "url": article.get("url"),
                "domain": article.get("domain"),
                "seendate": article.get("seendate"),
                "language": language,
                "sourcecountry": article.get("sourcecountry"),
                "socialimage": article.get("socialimage"),
            }
        )

    return parsed_rows