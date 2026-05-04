from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/ingestion")
sys.path.append("/opt/airflow/processing")

default_args = {
    'owner': 'akshay',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'opensky_bronze_to_silver_pipeline',
    default_args=default_args,
    description='End-to-end pipeline: Ingest -> Validate -> Transform -> Silver',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bronze', 'silver', 'opensky']
)

def run_ingestion():
    from fetch_opensky import fetch_opensky, validate_and_upload
    states = fetch_opensky()
    result = validate_and_upload(states)
    if not result:
        raise ValueError("Bronze upload produced no output file")


def run_silver_transform():
    from bronze_to_silver import run
    run()


# Task 1: Python ingestion (Bronze)
ingest_task = PythonOperator(
    task_id='fetch_validate_upload_bronze',
    python_callable=run_ingestion,
    dag=dag,
)

# Task 2: Bronze to Silver transformation
transform_task = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=run_silver_transform,
    dag=dag,
)

ingest_task >> transform_task
