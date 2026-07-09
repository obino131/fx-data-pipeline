with ranked_rates as (
    select
        base_currency,
        target_currency,
        rate_date,
        rate,
        rate_change,
        rate_change_pct,
        row_number() over (
            partition by base_currency, target_currency
            order by rate_date desc
        ) as rn
    from {{ ref('int_fx_rate_changes') }}
)

select
    base_currency,
    target_currency,
    rate_date,
    rate,
    rate_change,
    rate_change_pct
from ranked_rates
where rn = 1