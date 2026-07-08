-- DDL для raw-слоя. dbt отвечает только за staging/marts (source -> dwh),
-- сырую таблицу создаёт и наполняет пайплайн загрузки (Airflow), поэтому
-- она сознательно вынесена за пределы dbt-проекта.

CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.events
(
    event_id       String,
    event_ts       DateTime,
    seller_id      String,
    event_type     LowCardinality(String),
    currency       FixedString(3),
    amount         Decimal(18, 2),
    signed_amount  Decimal(18, 2),
    direction      LowCardinality(String),
    source_system  LowCardinality(String),
    _loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(event_ts)
ORDER BY (seller_id, event_ts);
