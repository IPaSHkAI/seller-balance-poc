"""
CLI-обёртка над pipeline.loader.load_all — полная загрузка CSV в raw.events.

Используется в CI (job dbt в .github/workflows/ci.yml) и для быстрых локальных
прогонов без Airflow: python scripts/load_all.py --csv data/events.csv
"""
from __future__ import annotations

import argparse

import clickhouse_connect

from pipeline.loader import load_all


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="data/events.csv")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=8123)
    p.add_argument("--user", default="default")
    p.add_argument("--password", default="")
    args = p.parse_args()

    client = clickhouse_connect.get_client(
        host=args.host, port=args.port, username=args.user, password=args.password
    )
    n = load_all(client, args.csv)
    print(f"Loaded {n} rows into raw.events")


if __name__ == "__main__":
    main()
