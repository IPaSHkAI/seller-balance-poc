{{ config(materialized='view') }}

-- Staging layer: чистим и дедуплицируем сырые события.
-- Дедупликация нужна, потому что источники "at-least-once" (ретраи продюсеров,
-- повторная доставка) — на проде это реальный сценарий, не гипотетический.

with source as (
    select * from {{ source('raw', 'events') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by event_id
            order by event_ts
        ) as rn
    from source
    where seller_id != ''
      and event_id is not null
      and event_ts is not null
)

select
    event_id,
    event_ts,
    seller_id,
    event_type,
    currency,
    amount,
    signed_amount,
    direction,
    source_system
from dedup
where rn = 1
