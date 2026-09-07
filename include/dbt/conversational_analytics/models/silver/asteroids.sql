select
    nullif(asteroid_id, '')::integer          as asteroid_id,
    asteroid_name,
    asteroid_belt,
    designation,
    nullif(diameter_km, '')::numeric(12, 2)   as diameter_km,
    nullif(is_mining_colony, '')::boolean     as is_mining_colony,
    nullif(has_station, '')::boolean          as has_station
from {{ source('bronze', 'reference__asteroids') }}
