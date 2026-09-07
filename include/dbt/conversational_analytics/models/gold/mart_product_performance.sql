with sales as (
    select
        product_sku,
        count(distinct order_id)        as order_count,
        sum(quantity)                   as units_sold,
        sum(line_revenue_credits)       as revenue_credits,
        avg(unit_price_credits)         as avg_unit_price_credits,
        avg(unit_price_variance_credits) as avg_price_variance_credits
    from {{ ref('fct_order_items') }}
    group by 1
),

support as (
    select
        product_sku,
        count(*)                                               as ticket_count,
        count(*) filter (where priority in ('high', 'urgent'))  as high_priority_ticket_count,
        avg(satisfaction_rating)                               as avg_satisfaction_rating
    from {{ ref('fct_support_tickets') }}
    where product_sku is not null
    group by 1
),

refunds as (
    select
        item.product_sku,
        count(distinct orders.order_id) as refunded_order_count
    from {{ ref('fct_order_items') }} as item
    inner join {{ ref('fct_orders') }} as orders on orders.order_id = item.order_id
    where orders.was_refunded
    group by 1
)

select
    product.product_sku,
    product.product_name,
    product.category_name,
    product.parent_category_name,
    product.is_hazardous,
    product.requires_cold_chain,
    product.base_price_credits,

    coalesce(sales.order_count, 0)          as order_count,
    coalesce(sales.units_sold, 0)           as units_sold,
    coalesce(sales.revenue_credits, 0)      as revenue_credits,
    sales.avg_unit_price_credits,
    sales.avg_price_variance_credits,

    coalesce(support.ticket_count, 0)               as ticket_count,
    coalesce(support.high_priority_ticket_count, 0) as high_priority_ticket_count,
    support.avg_satisfaction_rating,

    coalesce(refunds.refunded_order_count, 0)       as refunded_order_count,
    case
        when coalesce(sales.order_count, 0) = 0 then null
        else refunds.refunded_order_count::numeric / sales.order_count
    end                                             as refund_rate,
    case
        when coalesce(sales.units_sold, 0) = 0 then null
        else coalesce(support.ticket_count, 0) * 100.0 / sales.units_sold
    end                                             as tickets_per_hundred_units
from {{ ref('dim_product') }} as product
left join sales   on sales.product_sku = product.product_sku
left join support on support.product_sku = product.product_sku
left join refunds on refunds.product_sku = product.product_sku
