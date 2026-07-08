-- Singular-тест: net_change должен точно равняться inflow - outflow.
-- Тест "падает", если запрос возвращает хотя бы одну строку.
select *
from {{ ref('seller_balance_daily') }}
where abs(net_change - (inflow - outflow)) > 0.01
