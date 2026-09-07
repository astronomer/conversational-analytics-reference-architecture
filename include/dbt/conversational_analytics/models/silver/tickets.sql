{#
  product_sku is deliberately nullable: about 4 percent of tickets name no
  product, and there is no relationships test on the column.
#}
select
    ticket_id,
    customer_id,
    order_id,
    nullif(product_sku, '')                     as product_sku,
    subject,
    body,
    ticket_status,
    priority,
    channel,
    nullif(created_at, '')::timestamptz         as created_at,
    nullif(updated_at, '')::timestamptz         as updated_at,
    nullif(satisfaction_rating, '')::integer    as satisfaction_rating,
    length(body)                                as body_chars
from {{ source('bronze', 'zendesk__tickets') }}
