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

1. **Fetches data** from EventRegistry API every 30 seconds
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
