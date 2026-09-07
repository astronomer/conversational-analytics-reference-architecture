select
    nullif(category_id, '')::integer         as category_id,
    category_name,
    nullif(parent_category_id, '')::integer  as parent_category_id,
    nullif(category_level, '')::integer      as category_level,
    description,
    nullif(is_active, '')::boolean           as is_active
from {{ source('bronze', 'reference__product_categories') }}
