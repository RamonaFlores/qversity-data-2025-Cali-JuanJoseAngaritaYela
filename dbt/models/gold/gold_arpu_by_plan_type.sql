{{ config(materialized='table') }}

-- Join customers with their billing information
with joined as (
    select
        c.customer_id,
        c.plan_type,
        b.monthly_bill_usd
    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b
        using (customer_id)

    -- Only include records with a non-null monthly bill
    where b.monthly_bill_usd is not null
),

-- Group by plan type and calculate ARPU (Average Revenue Per User)
grouped as (
    select
        plan_type,
        round(avg(monthly_bill_usd), 2) as arpu
    from joined
    group by plan_type
)

-- Final result sorted by ARPU in descending order
select *
from grouped
order by arpu desc
