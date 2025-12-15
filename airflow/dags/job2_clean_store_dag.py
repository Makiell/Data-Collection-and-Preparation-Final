from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow')

from src.job2_cleaner import clean_and_store
from src.db_utils import DatabaseManager

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 14),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_cleaning_task(**context):
    db_manager = DatabaseManager(db_path='/opt/airflow/data/app.db')

    result = clean_and_store(
        kafka_servers='kafka:29092',
        topic='raw_events',
        db_manager=db_manager
    )

    context['task_instance'].xcom_push(key='cleaning_result', value=result)
    return result

def log_cleaning_results(**context):
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='clean_and_store_to_db', key='cleaning_result')

    if result:
        print("=" * 60)
        print(
            f"Messages: {result.get('messages_processed', 0)} | "
            f"Inserted: {result.get('events_inserted', 0)} | "
            f"Total: {result.get('total_events_in_db', 0)}"
        )
        print("=" * 60)
    else:
        print("No result from cleaning task")

with DAG(
    dag_id='job2_clean_store',
    default_args=default_args,
    description='Hourly cleaning and storage',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    clean_and_store_task = PythonOperator(
        task_id='clean_and_store_to_db',
        python_callable=run_cleaning_task
    )

    log_results_task = PythonOperator(
        task_id='log_cleaning_results',
        python_callable=log_cleaning_results
    )

    clean_and_store_task >> log_results_task
