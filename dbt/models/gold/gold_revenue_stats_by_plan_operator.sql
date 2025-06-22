{{ config(materialized='table') }}

with joined as (
    select
        c.plan_type,
        c.operator,
        b.monthly_bill_usd
    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b using (customer_id)
    where b.monthly_bill_usd is not null
)

select
    plan_type,
    operator,
    avg(monthly_bill_usd) as avg_revenue,
    percentile_cont(0.5) within group (order by monthly_bill_usd) as median_revenue
from joined
group by plan_type, operator
order by plan_type, operator
