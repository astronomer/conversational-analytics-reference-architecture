with items as (
    select
        order_id,
        count(*)                    as line_count,
        sum(quantity)               as unit_count,
        sum(line_revenue_credits)   as line_revenue_credits
    from {{ ref('order_items') }}
    group by 1
),

payment as (
    select order_id, charge_id, payment_status, payment_method, amount_credits
    from {{ ref('payments') }}
),

refund as (
    select order_id, sum(amount_credits) as refunded_credits, count(*) as refund_count
    from {{ ref('refunds') }}
    group by 1
),

shipment as (
    select order_id, shipment_id, shipped_at, delivered_at, delivery_days, shipment_status
    from {{ ref('shipments') }}
)

select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.ordered_at,
    orders.ordered_at::date                            as ordered_date,
    orders.route_id,
    route.route_name,
    coalesce(
        'hub-' || orders.destination_hub_id::text,
        'station-' || orders.destination_station_id::text
    )                                                  as destination_location_key,
    orders.destination_name,
    orders.revenue_credits,
    coalesce(items.line_count, 0)                      as line_count,
    coalesce(items.unit_count, 0)                      as unit_count,
    payment.charge_id,
    payment.payment_status,
    payment.payment_method,
    coalesce(refund.refunded_credits, 0)               as refunded_credits,
    coalesce(refund.refund_count, 0)                   as refund_count,
    refund.refunded_credits is not null                as was_refunded,
    shipment.shipment_id,
    shipment.shipped_at,
    shipment.delivered_at,
    shipment.delivery_days,
    shipment.shipment_status,
    case
        when shipment.delivery_days is null then null
        else shipment.delivery_days <= route.avg_transit_days
    end                                                as delivered_on_time
from {{ ref('orders') }} as orders
left join items    on items.order_id = orders.order_id
left join payment  on payment.order_id = orders.order_id
left join refund   on refund.order_id = orders.order_id
left join shipment on shipment.order_id = orders.order_id
left join {{ ref('routes') }} as route on route.route_id = orders.route_id
