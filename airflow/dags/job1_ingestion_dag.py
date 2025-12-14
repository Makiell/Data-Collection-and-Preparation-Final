from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow')

from src.job1_producer import run_producer


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_ingestion_task():
    run_producer(
        kafka_bootstrap_servers='kafka:29092',
        topic='raw_events',
        interval=60,
        duration=3600
    )


with DAG(
    'job1_continuous_ingestion',
    default_args=default_args,
    description='Continuous data ingestion from API to Kafka',
    schedule_interval=None,
    catchup=False,
) as dag:
    
    ingestion_task = PythonOperator(
        task_id='fetch_and_send_to_kafka',
        python_callable=run_ingestion_task,
    )
