{# CRM email addresses arrive upper-cased and whitespace-padded. #}
select
    contact_id,
    account_id,
    contact_name,
    lower(trim(email))              as email,
    title,
    nullif(created_date, '')::date  as created_date
from {{ source('bronze', 'salesforce__contacts') }}
