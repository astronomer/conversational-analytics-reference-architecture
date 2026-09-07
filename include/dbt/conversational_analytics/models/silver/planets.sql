select
    nullif(planet_id, '')::integer                  as planet_id,
    planet_name,
    planet_type,
    nullif(distance_from_sun_au, '')::numeric(10, 2) as distance_from_sun_au,
    nullif(orbital_period_days, '')::numeric(12, 2)  as orbital_period_days,
    nullif(is_habitable, '')::boolean               as is_habitable,
    nullif(has_orbital_station, '')::boolean        as has_orbital_station,
    nullif(has_surface_hub, '')::boolean            as has_surface_hub
from {{ source('bronze', 'reference__planets') }}
