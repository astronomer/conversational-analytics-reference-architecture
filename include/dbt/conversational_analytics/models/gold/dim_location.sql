with hubs as (
    select
        'hub-' || hub_id::text  as location_key,
        'hub'                   as location_type,
        hub_id                  as source_id,
        hub_name                as location_name,
        hub_type                as location_subtype,
        planet_id,
        capacity_tonnes,
        is_operational
    from {{ ref('hubs') }}
),

stations as (
    select
        'station-' || station_id::text,
        'station',
        station_id,
        station_name,
        station_type,
        planet_id,
        capacity_tonnes,
        is_operational
    from {{ ref('stations') }}
),

planets as (
    select
        'planet-' || planet_id::text,
        'planet',
        planet_id,
        planet_name,
        planet_type,
        planet_id,
        null::integer,
        true
    from {{ ref('planets') }}
),

moons as (
    select
        'moon-' || moon_id::text,
        'moon',
        moon_id,
        moon_name,
        null,
        planet_id,
        null::integer,
        true
    from {{ ref('moons') }}
),

asteroids as (
    select
        'asteroid-' || asteroid_id::text,
        'asteroid',
        asteroid_id,
        asteroid_name,
        asteroid_belt,
        null::integer,
        null::integer,
        true
    from {{ ref('asteroids') }}
),

unioned as (
    select * from hubs
    union all select * from stations
    union all select * from planets
    union all select * from moons
    union all select * from asteroids
)

select
    unioned.location_key,
    unioned.location_type,
    unioned.source_id,
    unioned.location_name,
    unioned.location_subtype,
    unioned.planet_id,
    planet.planet_name,
    planet.is_habitable as planet_is_habitable,
    unioned.capacity_tonnes,
    unioned.is_operational
from unioned
left join {{ ref('planets') }} as planet on planet.planet_id = unioned.planet_id
