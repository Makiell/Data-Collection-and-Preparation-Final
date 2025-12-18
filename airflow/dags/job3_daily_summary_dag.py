from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow')

from src.job3_analytics import compute_daily_analytics


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_analytics_task():
    """
    Wrapper function to run the daily analytics job
    """
    compute_daily_analytics(db_path='/opt/airflow/data/app.db')


with DAG(
    'job3_daily_summary',
    default_args=default_args,
    description='Daily analytics job: read SQLite, compute summary, write results',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    analytics_task = PythonOperator(
        task_id='compute_daily_summary',
        python_callable=run_analytics_task,
    )

