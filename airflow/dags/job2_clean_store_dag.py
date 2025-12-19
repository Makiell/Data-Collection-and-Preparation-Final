from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow')

from src.job2_cleaner import consume_and_clean_from_kafka


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_cleaning_task():
    consume_and_clean_from_kafka(
        kafka_bootstrap_servers='kafka:29092',
        topic='raw_events',
        db_path='/opt/airflow/data/app.db'
    )


with DAG(
    'job2_hourly_cleaning',
    default_args=default_args,
    description='Hourly batch job: read Kafka, clean data, write to SQLite',
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    
    cleaning_task = PythonOperator(
        task_id='consume_clean_store',
        python_callable=run_cleaning_task,
    )
