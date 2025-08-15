-- Load CSV (psql \copy example):
-- \copy raw.events FROM 'data/events.csv' WITH (FORMAT csv, HEADER true);

WITH agg AS (
    SELECT
        date_trunc('day', event_ts)::date AS dt,
        seller_id,
        sum(CASE WHEN signed_amount > 0 THEN signed_amount ELSE 0 END) AS inflow,
        sum(CASE WHEN signed_amount < 0 THEN -signed_amount ELSE 0 END) AS outflow,
        sum(signed_amount) AS net_change
    FROM raw.events
    GROUP BY 1,2
),
running AS (
    SELECT
        a.dt, a.seller_id, a.inflow, a.outflow, a.net_change,
        sum(a.net_change) OVER (PARTITION BY a.seller_id ORDER BY a.dt
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
    FROM agg a
)
INSERT INTO dwh.seller_balance_daily (dt, seller_id, inflow, outflow, net_change, running_balance)
SELECT * FROM running
ON CONFLICT (seller_id, dt) DO UPDATE
SET inflow = EXCLUDED.inflow,
    outflow = EXCLUDED.outflow,
    net_change = EXCLUDED.net_change,
    running_balance = EXCLUDED.running_balance;
