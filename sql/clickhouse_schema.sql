-- ClickHouse schema for seller balance PoC
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS raw.events
(
    event_id String,
    event_ts DateTime,
    seller_id String,
    event_type LowCardinality(String),
    currency FixedString(3),
    amount Decimal(18,2),
    signed_amount Decimal(18,2),
    direction LowCardinality(String),
    source_system LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(event_ts)
ORDER BY (seller_id, event_ts);

-- DWH facts table (daily balance movement per seller)
CREATE TABLE IF NOT EXISTS dwh.seller_balance_daily
(
    dt Date,
    seller_id String,
    inflow Decimal(18,2),
    outflow Decimal(18,2),
    net_change Decimal(18,2),
    running_balance Decimal(18,2)
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY (seller_id, dt);
