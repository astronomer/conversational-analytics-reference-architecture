select
    order_item_id,
    order_id,
    nullif(product_sku, '')                          as product_sku,
    nullif(quantity, '')::integer                    as quantity,
    nullif(unit_price_credits, '')::numeric(18, 2)   as unit_price_credits,
    nullif(quantity, '')::integer
        * nullif(unit_price_credits, '')::numeric(18, 2) as line_revenue_credits
from {{ source('bronze', 'appdb__order_items') }}
