select
    nullif(moon_id, '')::integer               as moon_id,
    moon_name,
    nullif(planet_id, '')::integer             as planet_id,
    nullif(orbital_radius_km, '')::numeric(14, 2) as orbital_radius_km,
    nullif(is_habitable, '')::boolean          as is_habitable,
    nullif(has_orbital_station, '')::boolean   as has_orbital_station,
    nullif(has_surface_hub, '')::boolean       as has_surface_hub
from {{ source('bronze', 'reference__moons') }}
