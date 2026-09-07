select
    nullif(station_id, '')::integer           as station_id,
    station_name,
    station_type,
    nullif(planet_id, '')::integer            as planet_id,
    nullif(moon_id, '')::integer              as moon_id,
    nullif(asteroid_id, '')::integer          as asteroid_id,
    nullif(orbital_altitude_km, '')::integer  as orbital_altitude_km,
    nullif(capacity_tonnes, '')::integer      as capacity_tonnes,
    nullif(is_operational, '')::boolean       as is_operational,
    nullif(commissioned_date, '')::date       as commissioned_date
from {{ source('bronze', 'reference__space_stations') }}
