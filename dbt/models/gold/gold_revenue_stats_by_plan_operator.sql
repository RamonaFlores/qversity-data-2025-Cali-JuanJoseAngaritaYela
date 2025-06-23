{{ config(materialized='table') }}

-- Join customer data with billing information
with joined as (
    select
        c.plan_type,
        c.operator,
        b.monthly_bill_usd
    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b using (customer_id)

    -- Only include records with non-null billing amounts
    where b.monthly_bill_usd is not null
)

-- Aggregate average and median revenue per plan type and operator
select
    plan_type,
    operator,

    -- Average monthly bill per group
    avg(monthly_bill_usd) as avg_revenue,

    -- Median monthly bill per group
    percentile_cont(0.5) within group (order by monthly_bill_usd) as median_revenue

from joined
group by plan_type, operator
order by plan_type, operator
