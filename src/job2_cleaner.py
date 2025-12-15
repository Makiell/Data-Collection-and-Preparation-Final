import json
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_articles(articles: list) -> list:
    if not articles:
        return []

    df = pd.DataFrame(articles)

    df["uri"] = df.get("uri", pd.Series()).fillna("").astype(str)
    df["title"] = df.get("title", pd.Series()).fillna("").astype(str).str.strip()
    df["url"] = df.get("url", pd.Series()).fillna("").astype(str).str.strip()
    df["language"] = df.get("lang", pd.Series()).fillna("unknown").astype(str).str.lower()

    df["source"] = df.get("source", pd.Series()).apply(
        lambda x: x.get("title", "") if isinstance(x, dict) else ""
    ).str.strip()

    df["published_at"] = df.get("dateTime", pd.Series()).fillna("").astype(str)

    df = df[
        (df["uri"] != "") &
        (df["title"].str.len() >= 5) &
        (df["url"].str.startswith("http"))
    ]

    df = df.drop_duplicates(subset=["uri"])

    return df[["uri", "title", "url", "source", "published_at", "language"]].to_dict("records")

def consume_from_kafka(kafka_servers, topic, timeout_ms=10000):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
        group_id=f"cleaner-{datetime.now().timestamp()}"
    )

    all_events = []
    messages = 0

    for msg in consumer:
        messages += 1
        raw = msg.value
        articles = raw.get("data", {}).get("articles", {}).get("results", [])
        cleaned = clean_articles(articles)
        all_events.extend(cleaned)

    consumer.close()
    logger.info(f"Consumed {messages} messages, cleaned {len(all_events)} events")
    return all_events

def clean_and_store(kafka_servers, topic, db_manager):
    db_manager.create_events_table()

    events = consume_from_kafka(kafka_servers, topic)
    inserted = db_manager.insert_events(events) if events else 0

    total_events = db_manager.get_event_count()

    return {
        "messages_processed": len(events),
        "events_inserted": inserted,
        "total_events_in_db": total_events,
        "timestamp": datetime.now().isoformat()
    }
