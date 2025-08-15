-- Simple DQ checks for Greenplum/Postgres
-- 1) Duplicates by event_id
SELECT event_id, count(*) FROM raw.events GROUP BY 1 HAVING count(*) > 1;

-- 2) Nulls in key fields
SELECT
  sum(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_id,
  sum(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END) AS null_event_ts,
  sum(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id
FROM raw.events;

-- 3) Amount sanity
SELECT * FROM raw.events WHERE amount > 1000000;
