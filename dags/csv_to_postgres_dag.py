from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

sys.path.append("/opt/airflow/scripts")

from pipeline import (
    get_execution_plan,
    create_batch,
    run_preflight,
    load_staging_table,
    load_core_table,
    finish_existing_batch,
    fail_existing_batch,
)


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def mark_batch_failed(context):
    ti = context["ti"]

    batch_id = ti.xcom_pull(task_ids="start_batch")

    if not batch_id:
        return

    exception = context.get("exception")
    reason = context.get("reason")

    if exception:
        error_message = str(exception)
    elif reason:
        error_message = str(reason)
    else:
        error_message = f"Task failed: {ti.task_id}, state={ti.state}"

    fail_existing_batch(batch_id, error_message)


config, levels = get_execution_plan()
files_config = config["files"]

with DAG(
    dag_id="csv_to_postgres",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=mark_batch_failed,
    tags=["etl", "csv", "postgres"],
) as dag:

    start_batch = PythonOperator(
        task_id="start_batch",
        python_callable=create_batch,
    )

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=run_preflight,
    )

    finish_batch = PythonOperator(
        task_id="finish_batch",
        python_callable=finish_existing_batch,
        op_kwargs={
            "batch_id": "{{ ti.xcom_pull(task_ids='start_batch') }}"
        },
    )

    staging_tasks = {}
    core_tasks = {}

    # ---------- STAGING ----------
    with TaskGroup(group_id="staging") as staging_group:
        for file_name in files_config.keys():
            staging_tasks[file_name] = PythonOperator(
                task_id=f"staging_{file_name}",
                python_callable=load_staging_table,
                op_kwargs={
                    "file_name": file_name,
                    "batch_id": "{{ ti.xcom_pull(task_ids='start_batch') }}",
                },
            )

     # ---------- CORE -----------
    with TaskGroup(group_id="core") as core_group:
        for file_name in files_config.keys():
            core_tasks[file_name] = PythonOperator(
                task_id=f"core_{file_name}",
                python_callable=load_core_table,
                op_kwargs={
                    "file_name": file_name,
                    "batch_id": "{{ ti.xcom_pull(task_ids='start_batch') }}",
                },
            )

    # ---------- DEPENDENCIES ----------
    for file_name, cfg in files_config.items():
        staging_tasks[file_name] >> core_tasks[file_name]

        for dependency in cfg.get("depends_on", []):
            core_tasks[dependency] >> core_tasks[file_name]

    # ---------- PIPELINE FLOW ----------
    start_batch >> staging_group

    for task in core_tasks.values():
        task >> finish_batch
