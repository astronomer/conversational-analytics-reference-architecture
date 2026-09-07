select
    customer.customer_id,
    customer.company_name,
    customer.contact_name,
    customer.email,
    customer.customer_tier,
    customer.is_active,
    customer.signup_date,
    customer.account_id,
    customer.industry,
    customer.annual_revenue_credits,
    customer.home_planet_id,
    planet.planet_name    as home_planet_name,
    planet.is_habitable   as home_planet_is_habitable
from {{ ref('customers') }} as customer
left join {{ ref('planets') }} as planet on planet.planet_id = customer.home_planet_id
