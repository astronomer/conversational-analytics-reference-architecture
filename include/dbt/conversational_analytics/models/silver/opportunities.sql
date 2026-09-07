select
    opportunity_id,
    account_id,
    opportunity_name,
    stage,
    nullif(amount_credits, '')::numeric(18, 2)  as amount_credits,
    nullif(probability, '')::integer            as probability,
    nullif(close_date, '')::date                as close_date,
    nullif(created_date, '')::date              as created_date,
    stage in ('closed_won')                     as is_won
from {{ source('bronze', 'salesforce__opportunities') }}
