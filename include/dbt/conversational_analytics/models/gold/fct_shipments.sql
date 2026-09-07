select
    shipment.shipment_id,
    shipment.order_id,
    orders.customer_id,
    shipment.route_id,
    route.route_name,
    route.origin_hub_id,
    route.destination_hub_id,
    route.distance_million_km,
    route.avg_transit_days,
    route.fuel_cost_credits,
    route.risk_level,
    shipment.shipped_at,
    shipment.shipped_at::date               as shipped_date,
    shipment.delivered_at,
    shipment.delivery_days,
    shipment.shipment_status,
    case
        when shipment.delivery_days is null then null
        else shipment.delivery_days <= route.avg_transit_days
    end                                     as delivered_on_time,
    case
        when shipment.delivery_days is null then null
        else shipment.delivery_days - route.avg_transit_days
    end                                     as days_vs_estimate
from {{ ref('shipments') }} as shipment
inner join {{ ref('orders') }} as orders on orders.order_id = shipment.order_id
left join {{ ref('routes') }} as route on route.route_id = shipment.route_id
