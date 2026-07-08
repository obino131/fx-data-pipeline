-- This test fails if any (base_currency, target_currency, rate_date) combination
-- appears more than once in stg_fx_rates — i.e. if deduplication logic breaks.
select
    base_currency,
    target_currency,
    rate_date,
    count(*) as row_count
from {{ ref('stg_fx_rates') }}
group by base_currency, target_currency, rate_date
having count(*) > 1
