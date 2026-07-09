-- This test fails if any currency pair has more than one row in fct_latest_fx_rates —
-- the mart is meant to expose exactly one (latest) row per (base_currency, target_currency).
select
    base_currency,
    target_currency,
    count(*) as row_count
from {{ ref('fct_latest_fx_rates') }}
group by base_currency, target_currency
having count(*) > 1
