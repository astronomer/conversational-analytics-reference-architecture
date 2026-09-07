select
    nullif(hub_id, '')::integer            as hub_id,
    hub_name,
    hub_type,
    nullif(planet_id, '')::integer         as planet_id,
    nullif(moon_id, '')::integer           as moon_id,
    nullif(asteroid_id, '')::integer       as asteroid_id,
    surface_coordinates,
    nullif(capacity_tonnes, '')::integer   as capacity_tonnes,
    nullif(is_operational, '')::boolean    as is_operational,
    nullif(commissioned_date, '')::date    as commissioned_date
from {{ source('bronze', 'reference__planetary_hubs') }}
