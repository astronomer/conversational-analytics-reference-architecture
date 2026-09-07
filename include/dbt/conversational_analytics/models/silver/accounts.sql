select
    account_id,
    account_name,
    customer_id,
    industry,
    account_tier,
    nullif(home_planet_id, '')::integer                 as home_planet_id,
    nullif(annual_revenue_credits, '')::numeric(18, 2)  as annual_revenue_credits,
    nullif(created_date, '')::date                      as created_date
from {{ source('bronze', 'salesforce__accounts') }}
