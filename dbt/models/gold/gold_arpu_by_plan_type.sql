{{ config(materialized='table') }}

with joined as (
    select
        c.customer_id,
        c.plan_type,
        b.monthly_bill_usd
    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b
        using (customer_id)
    where b.monthly_bill_usd is not null
),

grouped as (
    select
        plan_type,
        round(avg(monthly_bill_usd), 2) as arpu
    from joined
    group by plan_type
)

select *
from grouped
order by arpu desc
