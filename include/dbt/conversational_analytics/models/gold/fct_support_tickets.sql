with comments as (
    select
        ticket_id,
        count(*)                                             as comment_count,
        count(*) filter (where author_role = 'agent')         as agent_comment_count,
        min(created_at) filter (where author_role = 'agent')  as first_agent_reply_at
    from {{ ref('ticket_comments') }}
    group by 1
)

select
    ticket.ticket_id,
    ticket.customer_id,
    ticket.order_id,
    ticket.product_sku,
    product.product_name,
    product.category_id,
    ticket.subject,
    ticket.ticket_status,
    ticket.priority,
    ticket.channel,
    ticket.created_at,
    ticket.created_at::date                       as created_date,
    ticket.updated_at,
    ticket.satisfaction_rating,
    ticket.body_chars,
    coalesce(comments.comment_count, 0)           as comment_count,
    coalesce(comments.agent_comment_count, 0)     as agent_comment_count,
    comments.first_agent_reply_at,
    case
        when comments.first_agent_reply_at is null then null
        else extract(
            epoch from comments.first_agent_reply_at - ticket.created_at
        ) / 3600.0
    end                                           as hours_to_first_reply,
    ticket.ticket_status in ('solved', 'closed')  as is_resolved,
    ticket.product_sku is null                    as has_no_product_attached
from {{ ref('tickets') }} as ticket
left join comments on comments.ticket_id = ticket.ticket_id
left join {{ ref('products') }} as product on product.product_sku = ticket.product_sku
