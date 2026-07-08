-- Singular-тест: inflow и outflow по построению не могут быть отрицательными
-- (outflow берётся по abs()). Если тест что-то возвращает — значит трансформация сломана.
select *
from {{ ref('seller_balance_daily') }}
where inflow < 0 or outflow < 0
