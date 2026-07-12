from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

with DAG(
    dag_id="fx_pipeline",
    description="Daily FX ingestion -> dbt transform -> dbt test",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fx", "portfolio"],
) as dag:

    ingest_fx_rates = BashOperator(
        task_id="ingest_fx_rates",
        bash_command="cd /opt/airflow && python -m ingestion.fetch_fx",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test",
    )
    ingest_fx_rates >> dbt_run >> dbt_test