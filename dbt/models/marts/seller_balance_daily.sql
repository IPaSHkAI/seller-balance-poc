{{
    config(
        materialized='table',
        engine='MergeTree',
        order_by='(seller_id, dt)',
        partition_by='toYYYYMM(dt)'
    )
}}

-- Дневной баланс продавца: приток / отток / чистое изменение / накопительный баланс.
--
-- Решение: full refresh (materialized='table'), а не incremental.
-- running_balance — кумулятивная величина по всей истории продавца, и наивный
-- incremental-пересчёт только "новых" дней даёт неверный накопительный баланс
-- на границе инкремента. При объёме данных этого проекта (десятки продавцов,
-- месяцы истории) полный пересчёт занимает миллисекунды в ClickHouse — платить
-- за корректность полным refresh дешевле, чем городить carry-over логику.
-- При росте на порядки: пересчитывать инкрементально, храня "opening balance"
-- на начало каждого нового окна отдельной вспомогательной таблицей.

with events as (
    select * from {{ ref('stg_events') }}
),

daily as (
    select
        toDate(event_ts) as dt,
        seller_id,
        sumIf(signed_amount, signed_amount > 0) as inflow,
        abs(sumIf(signed_amount, signed_amount < 0)) as outflow,
        sum(signed_amount) as net_change
    from events
    group by dt, seller_id
)

select
    dt,
    seller_id,
    inflow,
    outflow,
    net_change,
    sum(net_change) over (
        partition by seller_id
        order by dt
        rows between unbounded preceding and current row
    ) as running_balance
from daily
