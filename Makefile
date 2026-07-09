.PHONY: up down logs data test dbt-run dbt-test dq-run load-all

up:
	docker compose up -d --build
	@echo "Airflow UI:  http://localhost:8080  (admin/admin)"
	@echo "ClickHouse:  http://localhost:8123"

down:
	docker compose down -v

logs:
	docker compose logs -f airflow

# Генерирует свежие синтетические данные (перезаписывает data/events.csv)
data:
	python3 data_generator/generate_events.py --sellers 25 --days 30 --seed 42 --out data/events.csv

# Юнит-тесты DQ-модуля (не требуют поднятой инфраструктуры)
test:
	PYTHONPATH=. pytest tests/ -v

# Ручной запуск dbt внутри уже поднятого контейнера airflow
dbt-run:
	docker compose exec airflow bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"

dbt-test:
	docker compose exec airflow bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt"

# Ручной прогон DQ-проверок по локальному CSV, без ClickHouse
dq-run:
	PYTHONPATH=. python3 dq/checks.py data/events.csv

# Полная загрузка CSV в raw.events без Airflow (для ручных прогонов / CI)
load-all:
	PYTHONPATH=. python3 scripts/load_all.py --csv data/events.csv --host localhost --port 8123
