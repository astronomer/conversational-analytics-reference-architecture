select
    subscription_id,
    customer_id,
    event_id,
    event_type,
    plan_name,
    nullif(amount_credits, '')::numeric(18, 2)  as amount_credits,
    "interval"                                  as billing_interval,
    status                                      as subscription_status,
    nullif(started_at, '')::timestamptz         as started_at
from {{ source('bronze', 'stripe__subscriptions') }}
