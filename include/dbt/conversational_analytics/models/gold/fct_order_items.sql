select
    item.order_item_id,
    item.order_id,
    item.product_sku,
    orders.customer_id,
    orders.ordered_at,
    orders.ordered_at::date         as ordered_date,
    item.quantity,
    item.unit_price_credits,
    item.line_revenue_credits,
    product.product_name,
    product.category_id,
    product.base_price_credits,
    item.unit_price_credits - product.base_price_credits as unit_price_variance_credits
from {{ ref('order_items') }} as item
inner join {{ ref('orders') }} as orders on orders.order_id = item.order_id
left join {{ ref('products') }} as product on product.product_sku = item.product_sku
