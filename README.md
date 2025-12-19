# Data Collection and Preparation - Final Project

## Overview

This project implements a complete streaming + batch data pipeline with three Airflow DAGs:
- **DAG 1**: Continuous data ingestion from API to Kafka
- **DAG 2**: Hourly batch job to clean and store data in SQLite
- **DAG 3**: Daily analytics job to compute summaries

---

## Project Setup

### Prerequisites
- Docker & Docker Compose

### Start Services

```bash
docker compose up --build -d
```

This will start:
- Kafka
- Airflow (webserver + scheduler)
- Kafka UI

### Access Web Interfaces

- **Airflow UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **Kafka UI**: http://localhost:8090

### Stop Services

```bash
docker compose down
```

---

## DAG 1: Continuous Data Ingestion

**File**: `airflow/dags/job1_ingestion_dag.py`

### What It Does

1. **Fetches data** from EventRegistry API every 60 seconds
2. **Sends messages** to Kafka topic `raw_events`
3. **Runs continuously** for 1 hour per execution

### Data Source

- **API**: EventRegistry (https://eventregistry.org)
- **Endpoint**: `/api/v1/article/getArticles`
- **Query**: Technology news articles (5 articles per request)

### How to Run

1. Open Airflow UI at http://localhost:8080
2. Find the DAG: `job1_continuous_ingestion`
3. Toggle it ON (unpause)
4. Click "Trigger DAG" to start manual execution

### Monitoring

- **Airflow Logs**: Check task logs in Airflow UI under the task `fetch_and_send_to_kafka`
- **Kafka Messages**: Open Kafka UI at http://localhost:8090, navigate to Topics → `raw_events` to see incoming messages

### Message Format

Each message sent to Kafka contains:
```json
{
  "timestamp": "2025-12-14T14:30:00.123456",
  "data": {
    "articles": {
      "results": [...]
    }
  }
}
```

---

## DAG 2: Hourly Data Cleaning and Storage

**File**: `airflow/dags/job2_clean_store_dag.py`

### What It Does

1. **Consumes messages** from Kafka topic `raw_events`
2. **Cleans data** using Pandas operations
3. **Stores cleaned data** in SQLite database (`data/app.db`)
4. **Runs hourly** on schedule

### Schedule

- **Cron**: `@hourly` (every hour at minute 0)

### Data Cleaning Process

All data cleaning is performed using **Pandas operations** (no vanilla Python loops):

1. **Extract nested fields**: Uses `df.apply()` to extract source information from nested JSON
2. **Rename columns**: Maps API fields to database schema
3. **Remove nulls**: Drops rows with missing `article_uri` using `df.dropna()`
4. **Remove duplicates**: Deduplicates based on `article_uri` using `df.drop_duplicates()`
5. **Fill missing values**: Uses `df.fillna()` with appropriate defaults
6. **Clean text fields**: Uses `str.strip()`, `str.split()`, `str.join()` for whitespace cleanup
7. **Type conversions**: Converts sentiment/wgt/relevance to numeric using `pd.to_numeric()`
8. **Normalize sentiment**: Clips values to [-1.0, 1.0] range using `df.clip()`

### Database Schema

**Table**: `events`

| Column | Type | Description |
|--------|------|-------------|
| article_uri | TEXT | Unique article identifier (primary key) |
| lang | TEXT | Article language code |
| datetime | TEXT | Publication datetime |
| data_type | TEXT | Type of data (e.g., "news") |
| url | TEXT | Article URL |
| title | TEXT | Article title |
| body | TEXT | Article body content |
| source_uri | TEXT | Source identifier |
| image_url | TEXT | Article image URL |
| sentiment | REAL | Sentiment score [-1.0, 1.0] |
| wgt | REAL | Article weight/importance |
| relevance | REAL | Relevance score |
| created_at | TIMESTAMP | Record insertion timestamp |

### How to Run

1. **Automatic**: DAG runs every hour automatically when enabled
2. **Manual**: In Airflow UI, find `job2_hourly_cleaning` and click "Trigger DAG"

### Consumer Configuration

- **Consumer Group**: `job2_hourly_cleaning`
- **Auto Offset Reset**: `earliest` (processes all unread messages)
- **Kafka Broker**: `kafka:29092` (internal Docker network)

---

## DAG 3: Daily Analytics and Summary

**File**: `airflow/dags/job3_daily_summary_dag.py`

### What It Does

1. **Reads cleaned data** from SQLite `events` table
2. **Computes aggregated metrics** grouped by date
3. **Writes summary** to `daily_summary` table
4. **Runs daily** on schedule

### Schedule

- **Cron**: `@daily` (every day at midnight)

### Analytics Computed

For each unique date in the dataset, the following metrics are calculated:

1. **Total Articles**: Count of articles published on that date
2. **Sentiment Statistics**:
   - Average sentiment across all articles
   - Minimum sentiment value
   - Maximum sentiment value
3. **Top Source**: Source with the most articles published
4. **Language Distribution**: JSON object with article counts per language

### Database Schema

**Table**: `daily_summary`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Auto-increment primary key |
| summary_date | TEXT | Date of the summary (YYYY-MM-DD) - UNIQUE |
| total_articles | INTEGER | Total number of articles |
| avg_sentiment | REAL | Average sentiment score |
| min_sentiment | REAL | Minimum sentiment score |
| max_sentiment | REAL | Maximum sentiment score |
| top_source | TEXT | Source URI with most articles |
| language_distribution | TEXT | JSON string of language counts |
| created_at | TIMESTAMP | When summary was created/updated |

### Example Summary Record

```json
{
  "summary_date": "2025-12-18",
  "total_articles": 125,
  "avg_sentiment": 0.234,
  "min_sentiment": -0.876,
  "max_sentiment": 0.956,
  "top_source": "bbc.co.uk",
  "language_distribution": "{\"eng\": 85, \"spa\": 25, \"fra\": 15}"
}
```

### How to Run

1. **Automatic**: DAG runs every day at midnight when enabled
2. **Manual**: In Airflow UI, find `job3_daily_summary` and click "Trigger DAG"

---

## Data Visualizations

**File**: `src/visualizations.py`

### Generate Plots

After running DAG 3 and collecting analytics data we create vizuals

```bash
python3 src/visualizations.py
```

1. **sentiment_trends.png** - Sentiment trends over time with min-max range
2. **daily_volume.png** - Daily article collection volume
3. **language_distribution.png** - Language distribution (bar + pie charts)
4. **language_heatmap.png** - Language distribution across dates
5. **sentiment_distribution.png** - Histogram of sentiment scores
6. **top_sources.png** - Top 10 news sources by article count

## Project Structure

```
project/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── src/
│   ├── __init__.py
│   ├── job1_producer.py
│   ├── job2_cleaner.py
│   ├── job3_analytics.py
│   ├── visualizations.py
│   └── db_utils.py
├── airflow/
│   └── dags/
│       ├── job1_ingestion_dag.py
│       ├── job2_clean_store_dag.py
│       └── job3_daily_summary_dag.py
├── data/
│   └── app.db
└── plots/
    ├── sentiment_trends.png
    ├── daily_volume.png
    ├── language_distribution.png
    ├── language_heatmap.png
    ├── sentiment_distribution.png
    └── top_sources.png
```