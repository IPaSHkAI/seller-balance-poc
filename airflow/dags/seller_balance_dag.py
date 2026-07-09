"""
Seller Balance Daily — Airflow DAG.
 
Заменяет версию-скелет, где DQ-проверка была функцией-заглушкой (dq_stub,
которая просто печатала строку и всегда возвращала "ok"). Здесь:
 
  * загрузка сырых событий идемпотентна по дате исполнения (data interval) —
    повторный запуск за тот же день не плодит дубли;
  * DQ-проверки реальные (dq.checks) и ПАДАЮТ таск, если находят проблему
    выше порога — а не молча логируют;
  * трансформации делегированы dbt (dbt run / dbt test), а не голым SQL-строкам
    внутри DAG;
  * на случай сбоя — колбэк уведомления (сейчас лог-стаб, в проде — вебхук
    в Slack/PagerDuty).
 
Пайплайн: load_raw_events -> dq_checks_raw -> dbt_run -> dbt_test
"""
from __future__ import annotations
 
import logging
from datetime import timedelta
 
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
 
log = logging.getLogger(__name__)
 
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DATA_PATH = "/opt/airflow/data/events.csv"
 
 
def _notify_failure(context) -> None:
    """Стаб уведомления о падении таска.
 
    В проде здесь был бы POST в Slack-вебхук или вызов PagerDuty API.
    Оставлено как явная точка расширения, а не молчаливый no-op —
    комментарий ниже описывает, что именно нужно подключить.
    """
    ti = context["task_instance"]
    log.error(
        "Task FAILED: dag=%s task=%s execution_date=%s — "
        "TODO: отправить алерт в Slack/PagerDuty вместо print/log",
        ti.dag_id, ti.task_id, context.get("logical_date"),
    )
 
 
def load_raw_events(**context) -> None:
    """Идемпотентно загружает срез events.csv за дату исполнения в raw.events.
 
    Сама логика загрузки — в pipeline.loader.load_day, общей для DAG и для
    standalone CLI-скрипта (scripts/load_all.py), который использует CI и
    ручные локальные прогоны. Это стандартный паттерн батчевой перезаливки
    партиции в ClickHouse (delete+insert), а не upsert "из воздуха".
    """
    import os
 
    import clickhouse_connect
 
    from pipeline.loader import load_day
 
    ds = context["ds"]  # дата исполнения DAG, формат YYYY-MM-DD
 
    client = clickhouse_connect.get_client(
        host=context["params"].get("clickhouse_host", "clickhouse"),
        port=8123,
        username="default",
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    n_rows = load_day(client, DATA_PATH, ds)
    log.info("Loaded %d rows for %s into raw.events", n_rows, ds)
    context["ti"].xcom_push(key="loaded_rows", value=n_rows)
 
 
def run_dq_checks_on_raw(**context) -> None:
    """Проверяет качество данных за конкретный день ДО того, как dbt их трансформирует.
 
    Это отдельный слой проверок поверх dbt-тестов: dbt test проверяет уже
    трансформированную витрину (schema tests + reconciliation), а этот таск —
    сырой срез сразу после загрузки. Если здесь падает — dbt даже не запустится
    на заведомо плохих данных.
    """
    import os
 
    import clickhouse_connect
    import pandas as pd
 
    from dq.checks import run_dq_checks_or_raise
 
    ds = context["ds"]
    client = clickhouse_connect.get_client(
        host=context["params"].get("clickhouse_host", "clickhouse"),
        port=8123,
        username="default",
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    result = client.query(
        f"SELECT * FROM raw.events WHERE toDate(event_ts) = '{ds}'"
    )
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
 
    report = run_dq_checks_or_raise(df, error_rate_threshold=0.02)
    log.info(report.summary())
    context["ti"].xcom_push(key="dq_report", value=report.to_dict())
 
 
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": _notify_failure,
}
 
with DAG(
    dag_id="seller_balance_daily",
    description="Ежедневный пересчёт баланса продавцов: load -> DQ -> dbt run -> dbt test",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["seller_balance", "dbt", "dq"],
) as dag:
 
    load_raw = PythonOperator(
        task_id="load_raw_events",
        python_callable=load_raw_events,
        params={"clickhouse_host": "clickhouse"},
    )
 
    dq_checks_raw = PythonOperator(
        task_id="dq_checks_raw",
        python_callable=run_dq_checks_on_raw,
        params={"clickhouse_host": "clickhouse"},
    )
 
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )
 
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )
 
    load_raw >> dq_checks_raw >> dbt_run >> dbt_test
 