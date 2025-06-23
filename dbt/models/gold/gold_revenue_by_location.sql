{{ config(materialized='table') }}

-- Join customer and billing data to associate geographic info with revenue
with joined as (
    select
        c.city,
        c.country,
        b.monthly_bill_usd
    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b
        using (customer_id)

    -- Only include records with non-null billing amounts
    where b.monthly_bill_usd is not null
),

-- Aggregate total revenue by country and city
grouped as (
    select
        country,
        city,

        -- Round total revenue to 2 decimal places
        round(sum(monthly_bill_usd), 2) as total_revenue
    from joined
    group by country, city
)

-- Final output: ranked city-level revenue contribution
select *
from grouped
order by total_revenue desc
