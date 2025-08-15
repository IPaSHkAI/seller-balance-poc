-- Greenplum/PostgreSQL schema for seller balance PoC
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
