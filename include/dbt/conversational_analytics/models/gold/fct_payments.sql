select
    payment.charge_id,
    payment.order_id,
    payment.customer_id,
    payment.event_id,
    payment.event_type,
    payment.amount_credits,
    payment.currency,
    payment.payment_status,
    payment.payment_method,
    payment.charged_at,
    payment.charged_at::date                     as charged_date,
    coalesce(refund.refunded_credits, 0)         as refunded_credits,
    refund.refunded_credits is not null          as was_refunded,
    payment.amount_credits - coalesce(refund.refunded_credits, 0) as net_credits
from {{ ref('payments') }} as payment
left join (
    select charge_id, sum(amount_credits) as refunded_credits
    from {{ ref('refunds') }}
    group by 1
) as refund on refund.charge_id = payment.charge_id
