-- Load CSV into raw.events (example command for clickhouse-client)
-- cat data/events.csv | clickhouse-client --query="INSERT INTO raw.events FORMAT CSVWithNames"

-- Build daily aggregates
INSERT INTO dwh.seller_balance_daily
SELECT
    toDate(event_ts) AS dt,
    seller_id,
    sumIf(signed_amount, signed_amount > 0) AS inflow,
    abs(sumIf(signed_amount, signed_amount < 0)) AS outflow,
    sum(signed_amount) AS net_change,
    -- Running balance by seller over time
    sum(net_change) OVER (PARTITION BY seller_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM raw.events
GROUP BY dt, seller_id
ORDER BY seller_id, dt;
