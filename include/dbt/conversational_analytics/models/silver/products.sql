select
    product_sku,
    nullif(product_id, '')::integer                 as product_id,
    product_name,
    nullif(category_id, '')::integer                as category_id,
    description,
    nullif(weight_kg, '')::numeric(12, 2)           as weight_kg,
    dimensions_cm,
    nullif(is_hazardous, '')::boolean               as is_hazardous,
    nullif(requires_cold_chain, '')::boolean        as requires_cold_chain,
    nullif(base_price_credits, '')::numeric(18, 2)  as base_price_credits,
    nullif(is_active, '')::boolean                  as is_active
from {{ source('bronze', 'reference__products') }}
