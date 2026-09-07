select
    refund_id,
    charge_id,
    order_id,
    event_id,
    event_type,
    nullif(amount_credits, '')::numeric(18, 2)  as amount_credits,
    reason,
    nullif(refunded_at, '')::timestamptz        as refunded_at
from {{ source('bronze', 'stripe__refunds') }}
