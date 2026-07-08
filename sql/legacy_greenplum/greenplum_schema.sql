-- Greenplum/PostgreSQL schema for seller balance PoC
-- Референсный вариант: этот проект в основном варианте построен на ClickHouse + dbt
-- (см. dbt/ и sql/clickhouse_raw_ddl.sql), но исходная идея была warehouse-agnostic.
-- Эти скрипты показывают, что тот же дизайн переносится на Greenplum/Postgres без
-- изменений в модели данных — только в диалекте SQL.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS raw.events
(
    event_id text PRIMARY KEY,
    event_ts timestamp NOT NULL,
    seller_id text NOT NULL,
    event_type text NOT NULL,
    currency char(3) NOT NULL,
    amount numeric(18,2) NOT NULL,
    signed_amount numeric(18,2) NOT NULL,
    direction text NOT NULL,
    source_system text NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.seller_balance_daily
(
    dt date NOT NULL,
    seller_id text NOT NULL,
    inflow numeric(18,2),
    outflow numeric(18,2),
    net_change numeric(18,2),
    running_balance numeric(18,2),
    PRIMARY KEY (seller_id, dt)
);
