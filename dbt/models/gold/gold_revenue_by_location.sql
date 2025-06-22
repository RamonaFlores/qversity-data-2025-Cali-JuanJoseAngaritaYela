{{ config(materialized='table') }}

with joined as (
    select
        c.city,
        c.country,
        b.monthly_bill_usd
    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b
        using (customer_id)
    where b.monthly_bill_usd is not null
),

grouped as (
    select
        country,
        city,
        round(sum(monthly_bill_usd), 2) as total_revenue
    from joined
    group by country, city
)

select *
from grouped
order by total_revenue desc
