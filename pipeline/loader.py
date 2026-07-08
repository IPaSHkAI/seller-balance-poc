"""
Общая логика загрузки событий в ClickHouse.

Вынесена в отдельный модуль, чтобы Airflow DAG и standalone CLI-скрипт
(scripts/load_all.py, используется в CI и для ручных прогонов) не дублировали
одну и ту же логику загрузки в двух местах.
"""
from __future__ import annotations

import pandas as pd


def load_day(client, csv_path: str, ds: str) -> int:
    """Идемпотентно загружает срез CSV за конкретный день исполнения (ds='YYYY-MM-DD').

    Идемпотентность: сначала удаляем строки за этот день (ALTER TABLE ... DELETE),
    затем вставляем заново. Повторный запуск за тот же ds не создаёт дублей —
    это тот сценарий, который в версии-скелете вообще не был предусмотрен.
    """
    df = pd.read_csv(csv_path, dtype=str)
    df["event_date"] = pd.to_datetime(df["event_ts"]).dt.strftime("%Y-%m-%d")
    day_slice = df[df["event_date"] == ds].drop(columns=["event_date"])

    client.command(f"ALTER TABLE raw.events DELETE WHERE toDate(event_ts) = '{ds}'")
    if not day_slice.empty:
        client.insert_df("raw.events", day_slice)
    return len(day_slice)


def load_all(client, csv_path: str) -> int:
    """Полная (не инкрементальная по дате) загрузка всего файла целиком.

    Используется в CI и для быстрых локальных прогонов, где не нужна семантика
    "загрузить срез за дату исполнения DAG" — только проверить, что dbt-модели
    и DQ-проверки работают корректно на полном датасете.
    """
    df = pd.read_csv(csv_path, dtype=str)
    client.command("TRUNCATE TABLE raw.events")
    if not df.empty:
        client.insert_df("raw.events", df)
    return len(df)
