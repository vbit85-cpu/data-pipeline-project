import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.etl import run_etl

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="csv_to_postgres",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    task_etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl
    )
