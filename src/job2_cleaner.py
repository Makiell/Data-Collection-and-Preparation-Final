import json
from kafka import KafkaConsumer
import pandas as pd
from src.db_utils import get_db_connection, create_tables


def consume_and_clean_from_kafka(kafka_bootstrap_servers='localhost:29092', topic='raw_events', db_path='data/app.db'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='job2_hourly_cleaning',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    print(f"Connected to Kafka, consuming from topic: {topic}")
    
    create_tables(db_path)
    conn = get_db_connection(db_path)
    
    message_count = 0
    all_articles = []
    
    try:
        for message in consumer:
            message_count += 1
            print(f"Processing message #{message_count}")
            
            raw_data = message.value
            articles_data = raw_data.get('data', {}).get('articles', {}).get('results', [])
            
            if not articles_data:
                print(f"No articles found in message #{message_count}")
                continue

            all_articles.extend(articles_data)
        
        if not all_articles:
            print("No articles to process")
            return 0
        
        print(f"Collected {len(all_articles)} total articles from {message_count} messages")
        print("Creating DataFrame...")
        df = pd.DataFrame(all_articles)
        
        print("Starting data cleaning process...")
        df_cleaned = clean_with_pandas(df)
        
        print(f"Saving {len(df_cleaned)} cleaned articles to database...")
        df_cleaned.to_sql('events', conn, if_exists='append', index=False)

        print(f"Finished! Total messages: {message_count}, Total cleaned articles: {len(df_cleaned)}")

        print("="*70)
        print("SAMPLE OF DATA SAVED TO DATABASE (first 3 records):")
        print("="*70)
        cursor = conn.cursor()
        cursor.execute("SELECT article_uri, title, lang, sentiment, datetime FROM events ORDER BY created_at DESC LIMIT 3")
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"[{i}] Article URI: {row[0]}")
            print(f"    Title: {row[1][:80]}..." if len(row[1]) > 80 else f"    Title: {row[1]}")
            print(f"    Language: {row[2]}")
            print(f"    Sentiment: {row[3]:.3f}")
            print(f"    DateTime: {row[4]}")
        

        cursor.execute("SELECT COUNT(*) FROM events")
        total_in_db = cursor.fetchone()[0]
        
        print("="*70)
        print("DATABASE STATISTICS:")
        print(f"Total articles in database: {total_in_db}")
        print("="*70)
        
        return len(df_cleaned)
        
    except Exception as e:
        print(f"Error processing messages: {e}")
        return 0
    finally:
        consumer.close()
        conn.close()


def clean_with_pandas(df: pd.DataFrame) -> pd.DataFrame:
    print(f"Initial articles count: {len(df)}")
    
    print("Extracting source_uri from nested source field...")
    df['source_uri'] = df['source'].apply(lambda x: x.get('uri', '') if isinstance(x, dict) else '')

    print("Renaming columns...")
    df = df.rename(columns={
        'uri': 'article_uri',
        'dateTime': 'datetime',
        'dataType': 'data_type',
        'image': 'image_url'
    })

    columns = [
        'article_uri', 'lang', 'datetime', 'data_type', 'url', 'title', 
        'body', 'source_uri', 'image_url', 'sentiment', 'wgt', 'relevance'
    ]
    df = df[columns]

    before_drop = len(df)
    df = df.dropna(subset=['article_uri'])
    dropped = before_drop - len(df)
    if dropped > 0:
        print(f"Dropped {dropped} articles with missing article_uri")
    
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['article_uri'], keep='first')
    duplicates = before_dedup - len(df)
    if duplicates > 0:
        print(f"Removed {duplicates} duplicate articles")

    print("Filling missing values and cleaning text fields...")
    df['lang'] = df['lang'].fillna('unknown').str.strip().str[:10]
    df['datetime'] = df['datetime'].fillna('').astype(str).str.strip()
    df['data_type'] = df['data_type'].fillna('').astype(str).str.strip()
    df['url'] = df['url'].fillna('').astype(str).str.strip()
    df['body'] = df['body'].fillna('').astype(str).str.strip()
    df['source_uri'] = df['source_uri'].fillna('').astype(str).str.strip()
    df['image_url'] = df['image_url'].fillna('').astype(str).str.strip()

    print("Removing excessive whitespace from body and title...")
    df['body'] = df['body'].str.split().str.join(' ')
    df['title'] = df['title'].str.strip()
    df['article_uri'] = df['article_uri'].astype(str).str.strip()

    print("Converting numeric fields and normalizing sentiment...")
    df['sentiment'] = pd.to_numeric(df['sentiment'], errors='coerce').fillna(0.0)
    df['sentiment'] = df['sentiment'].clip(-1.0, 1.0)
    
    df['wgt'] = pd.to_numeric(df['wgt'], errors='coerce').fillna(0).astype(int)
    df['relevance'] = pd.to_numeric(df['relevance'], errors='coerce').fillna(0).astype(int)
    
    final_count = len(df)
    print("=== Cleaning completed ===")
    print(f"Final articles count: {final_count}")
    
    print(f"Cleaned {len(df)} articles using Pandas")
    
    return df
