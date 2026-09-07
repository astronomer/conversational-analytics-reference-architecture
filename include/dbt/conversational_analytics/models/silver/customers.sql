with app as (
    select
        customer_id,
        company_name,
        contact_name,
        nullif(trim(email), '')                  as email,
        customer_tier,
        nullif(home_planet_id, '')::integer      as home_planet_id,
        nullif(signup_date, '')::date            as signup_date,
        nullif(is_active, '')::boolean           as is_active
    from {{ source('bronze', 'appdb__customers') }}
),

crm as (
    select
        customer_id,
        account_id,
        industry,
        nullif(annual_revenue_credits, '')::numeric(18, 2) as annual_revenue_credits
    from {{ source('bronze', 'salesforce__accounts') }}
)

select
    app.customer_id,
    app.company_name,
    app.contact_name,
    app.email,
    app.customer_tier,
    app.home_planet_id,
    app.signup_date,
    app.is_active,
    crm.account_id,
    crm.industry,
    crm.annual_revenue_credits
from app
left join crm on crm.customer_id = app.customer_id
