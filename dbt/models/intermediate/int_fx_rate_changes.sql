with source_data as (
    select
        base_currency,
        target_currency,
        rate_date,
        rate,
        lag(rate) over (
            partition by base_currency, target_currency
            order by rate_date
        ) as previous_rate
    from {{ ref('stg_fx_rates') }}
)

select
    base_currency,
    target_currency,
    rate_date,
    rate,
    previous_rate,
    rate - previous_rate as rate_change,
    round((rate - previous_rate) / previous_rate * 100, 4) as rate_change_pct
from source_data
