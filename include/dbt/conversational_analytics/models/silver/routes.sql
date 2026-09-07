select
    nullif(route_id, '')::integer                     as route_id,
    route_name,
    nullif(origin_station_id, '')::integer            as origin_station_id,
    nullif(origin_hub_id, '')::integer                as origin_hub_id,
    nullif(destination_station_id, '')::integer       as destination_station_id,
    nullif(destination_hub_id, '')::integer           as destination_hub_id,
    nullif(distance_million_km, '')::numeric(12, 2)   as distance_million_km,
    nullif(avg_transit_days, '')::numeric(8, 2)       as avg_transit_days,
    nullif(fuel_cost_credits, '')::numeric(18, 2)     as fuel_cost_credits,
    risk_level,
    nullif(is_active, '')::boolean                    as is_active
from {{ source('bronze', 'reference__shipping_routes') }}
