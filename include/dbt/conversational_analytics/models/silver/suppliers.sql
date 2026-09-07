select
    nullif(supplier_id, '')::integer              as supplier_id,
    supplier_name,
    supplier_code,
    lower(trim(contact_email))                    as contact_email,
    contact_name,
    nullif(payment_terms_days, '')::integer       as payment_terms_days,
    nullif(reliability_rating, '')::numeric(4, 2) as reliability_rating,
    nullif(is_active, '')::boolean                as is_active,
    nullif(contracted_since, '')::date            as contracted_since
from {{ source('bronze', 'reference__suppliers') }}
