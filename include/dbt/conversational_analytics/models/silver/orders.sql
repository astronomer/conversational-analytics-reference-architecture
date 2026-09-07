select
    order_id,
    customer_id,
    order_status,
    nullif(ordered_at, '')::timestamptz              as ordered_at,
    nullif(route_id, '')::integer                    as route_id,
    nullif(destination_hub_id, '')::integer          as destination_hub_id,
    nullif(destination_station_id, '')::integer      as destination_station_id,
    destination_name,
    nullif(promotion_id, '')::integer                as promotion_id,
    nullif(revenue_credits, '')::numeric(18, 2)      as revenue_credits,
    currency
from {{ source('bronze', 'appdb__orders') }}
