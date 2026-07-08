Select DISTINCT ON (base_currency, target_currency, rate_date) 
base_currency, 
target_currency, 
rate_date, 
rate, 
source
from {{ source('bronze', 'bronze_fx_rates') }}
order by base_currency, target_currency, rate_date, ingested_at DESC