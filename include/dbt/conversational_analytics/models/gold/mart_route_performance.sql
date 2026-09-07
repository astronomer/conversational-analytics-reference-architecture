with shipped as (
    select
        route_id,
        count(*)                                              as shipment_count,
        count(*) filter (where delivered_on_time)              as on_time_count,
        count(*) filter (where delivered_on_time is not null)  as measurable_count,
        avg(delivery_days)                                     as avg_delivery_days,
        avg(days_vs_estimate)                                  as avg_days_vs_estimate,
        max(delivery_days)                                     as worst_delivery_days
    from {{ ref('fct_shipments') }}
    group by 1
),

revenue as (
    select route_id, sum(revenue_credits) as revenue_credits, count(*) as order_count
    from {{ ref('fct_orders') }}
    group by 1
)

select
    route.route_id,
    route.route_name,
    route.risk_level,
    route.distance_million_km,
    route.avg_transit_days                        as estimated_transit_days,
    route.fuel_cost_credits,
    origin.location_name                          as origin_hub_name,
    destination.location_name                     as destination_hub_name,

    coalesce(shipped.shipment_count, 0)           as shipment_count,
    shipped.avg_delivery_days                     as actual_avg_delivery_days,
    shipped.avg_days_vs_estimate,
    shipped.worst_delivery_days,
    case
        when coalesce(shipped.measurable_count, 0) = 0 then null
        else shipped.on_time_count::numeric / shipped.measurable_count
    end                                           as on_time_rate,

    coalesce(revenue.order_count, 0)              as order_count,
    coalesce(revenue.revenue_credits, 0)          as revenue_credits,
    route.fuel_cost_credits * coalesce(shipped.shipment_count, 0) as total_fuel_cost_credits,
    case
        when coalesce(revenue.revenue_credits, 0) = 0 then null
        else route.fuel_cost_credits * shipped.shipment_count / revenue.revenue_credits
    end                                           as fuel_cost_share_of_revenue
from {{ ref('routes') }} as route
left join shipped on shipped.route_id = route.route_id
left join revenue on revenue.route_id = route.route_id
left join {{ ref('dim_location') }} as origin
    on origin.location_key = 'hub-' || route.origin_hub_id::text
left join {{ ref('dim_location') }} as destination
    on destination.location_key = 'hub-' || route.destination_hub_id::text
