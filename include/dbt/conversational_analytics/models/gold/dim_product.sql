select
    product.product_sku,
    product.product_id,
    product.product_name,
    product.description,
    product.category_id,
    category.category_name,
    parent.category_name  as parent_category_name,
    product.weight_kg,
    product.dimensions_cm,
    product.is_hazardous,
    product.requires_cold_chain,
    product.base_price_credits,
    product.is_active
from {{ ref('products') }} as product
left join {{ ref('product_categories') }} as category
    on category.category_id = product.category_id
left join {{ ref('product_categories') }} as parent
    on parent.category_id = category.parent_category_id
