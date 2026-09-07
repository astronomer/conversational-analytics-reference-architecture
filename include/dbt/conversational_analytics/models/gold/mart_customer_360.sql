with window_end as (
    select date_day from {{ ref('dim_date') }} where is_window_end
),

order_activity as (
    select
        customer_id,
        count(*)                                              as order_count,
        sum(revenue_credits)                                  as lifetime_revenue_credits,
        sum(refunded_credits)                                 as lifetime_refunded_credits,
        avg(revenue_credits)                                  as avg_order_credits,
        min(ordered_at)::date                                 as first_order_date,
        max(ordered_at)::date                                 as last_order_date,
        count(*) filter (where order_status = 'cancelled')     as cancelled_order_count,
        count(*) filter (where delivered_on_time is false)     as late_delivery_count,
        count(*) filter (where delivered_on_time is not null)  as measurable_delivery_count
    from {{ ref('fct_orders') }}
    group by 1
),

support_activity as (
    select
        customer_id,
        count(*)                                                    as ticket_count,
        count(*) filter (where not is_resolved)                      as open_ticket_count,
        count(*) filter (where priority in ('high', 'urgent'))       as high_priority_ticket_count,
        avg(satisfaction_rating)                                     as avg_satisfaction_rating,
        avg(hours_to_first_reply)                                    as avg_hours_to_first_reply,
        max(created_at)::date                                        as last_ticket_date
    from {{ ref('fct_support_tickets') }}
    group by 1
),

top_category as (
    select distinct on (customer_id)
        customer_id,
        category.category_name,
        sum(item.line_revenue_credits) as category_revenue_credits
    from {{ ref('fct_order_items') }} as item
    left join {{ ref('product_categories') }} as category
        on category.category_id = item.category_id
    group by customer_id, category.category_name
    order by customer_id, sum(item.line_revenue_credits) desc
),

subscription_activity as (
    select
        customer_id,
        count(*)                                          as subscription_count,
        count(*) filter (where subscription_status = 'active') as active_subscription_count,
        sum(amount_credits) filter (where subscription_status = 'active') as active_subscription_credits
    from {{ ref('subscriptions') }}
    group by 1
)

select
    customer.customer_id,
    customer.company_name,
    customer.customer_tier,
    customer.industry,
    customer.is_active,
    customer.home_planet_name,
    customer.signup_date,

    coalesce(orders.order_count, 0)                     as order_count,
    coalesce(orders.lifetime_revenue_credits, 0)        as lifetime_revenue_credits,
    coalesce(orders.lifetime_refunded_credits, 0)       as lifetime_refunded_credits,
    coalesce(orders.lifetime_revenue_credits, 0)
        - coalesce(orders.lifetime_refunded_credits, 0) as lifetime_net_credits,
    orders.avg_order_credits,
    orders.first_order_date,
    orders.last_order_date,
    coalesce(orders.cancelled_order_count, 0)           as cancelled_order_count,
    case
        when coalesce(orders.measurable_delivery_count, 0) = 0 then null
        else orders.late_delivery_count::numeric / orders.measurable_delivery_count
    end                                                 as late_delivery_rate,

    coalesce(support.ticket_count, 0)                   as ticket_count,
    coalesce(support.open_ticket_count, 0)              as open_ticket_count,
    coalesce(support.high_priority_ticket_count, 0)     as high_priority_ticket_count,
    support.avg_satisfaction_rating,
    support.avg_hours_to_first_reply,
    support.last_ticket_date,

    top_category.category_name                          as top_category_name,
    coalesce(subs.subscription_count, 0)                as subscription_count,
    coalesce(subs.active_subscription_count, 0)         as active_subscription_count,
    coalesce(subs.active_subscription_credits, 0)       as active_subscription_credits,

    (window_end.date_day - orders.last_order_date)::integer  as days_since_last_order,
    (window_end.date_day - support.last_ticket_date)::integer as days_since_last_ticket
from {{ ref('dim_customer') }} as customer
cross join window_end
left join order_activity        as orders       on orders.customer_id = customer.customer_id
left join support_activity      as support      on support.customer_id = customer.customer_id
left join top_category                          on top_category.customer_id = customer.customer_id
left join subscription_activity as subs         on subs.customer_id = customer.customer_id
