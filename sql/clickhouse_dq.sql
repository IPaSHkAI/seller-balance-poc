-- Simple DQ checks
-- 1) Duplicates
SELECT seller_id, event_id, count(*) AS cnt
FROM raw.events
GROUP BY seller_id, event_id
HAVING cnt > 1;

-- 2) Nulls
SELECT countIf(isNull(event_id)) AS null_event_id,
       countIf(isNull(event_ts)) AS null_event_ts,
       countIf(isNull(seller_id)) AS null_seller_id
FROM raw.events;

-- 3) Outliers: suspiciously large amounts
SELECT *
FROM raw.events
WHERE amount > 1000000;
