select
    charge_id,
    order_id,
    customer_id,
    event_id,
    event_type,
    nullif(amount_credits, '')::numeric(18, 2)  as amount_credits,
    currency,
    payment_status,
    payment_method,
    nullif(charged_at, '')::timestamptz         as charged_at
from {{ source('bronze', 'stripe__charges') }}
