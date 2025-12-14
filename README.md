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
