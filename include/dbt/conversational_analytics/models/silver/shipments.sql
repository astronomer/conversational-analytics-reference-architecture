{#
  delivered_at is null while a shipment is in transit, so delivery_days is null too.
  A zero there would read as same-day delivery in the marts.
#}
select
    shipment_id,
    order_id,
    nullif(route_id, '')::integer          as route_id,
    nullif(spacecraft_id, '')::integer     as spacecraft_id,
    nullif(pilot_id, '')::integer          as pilot_id,
    nullif(shipped_at, '')::timestamptz    as shipped_at,
    nullif(delivered_at, '')::timestamptz  as delivered_at,
    shipment_status,
    case
        when nullif(delivered_at, '') is null then null
        else extract(
            day from nullif(delivered_at, '')::timestamptz
                   - nullif(shipped_at, '')::timestamptz
        )::integer
    end as delivery_days
from {{ source('bronze', 'appdb__shipments') }}
