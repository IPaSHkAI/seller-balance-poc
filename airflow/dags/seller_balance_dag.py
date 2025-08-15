from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def dq_stub(**context):
    # place your DQ checks here (row counts, null checks, duplicates)
    print("Run DQ checks...")
    return "ok"

with DAG(
    dag_id="seller_balance_daily",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["poc", "seller_balance"],
) as dag:

    # Example: load CSV to ClickHouse (requires configured clickhouse-client and connection)
    load_raw = BashOperator(
        task_id="load_raw_events",
        bash_command=(
            "cat /opt/airflow/dags/data/events.csv | "
            "clickhouse-client --query="INSERT INTO raw.events FORMAT CSVWithNames""
        )
    )

    run_transforms = BashOperator(
        task_id="run_transforms",
        bash_command=(
            "clickhouse-client --query="$(cat /opt/airflow/dags/sql/clickhouse_transforms.sql)""
        )
    )

    dq = PythonOperator(
        task_id="dq_checks",
        python_callable=dq_stub
    )

    load_raw >> run_transforms >> dq
