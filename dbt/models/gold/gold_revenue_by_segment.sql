{{ config(materialized='table') }}

-- Base CTE: join customer and billing data, and classify into ranges
with base as (
    select
        c.customer_id,
        b.monthly_bill_usd,

        -- Age range classification
        case
            when c.age < 25 then '<25'
            when c.age between 25 and 40 then '25-40'
            when c.age between 41 and 60 then '41-60'
            else '>60'
        end as age_range,

        -- Plan type
        c.plan_type,

        -- Credit score range classification
        case
            when c.credit_score < 500 then '<500'
            when c.credit_score between 500 and 700 then '500-700'
            when c.credit_score between 701 and 850 then '701-850'
            else '>850'
        end as credit_score_range

    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b
        using (customer_id)

    -- Only include records with non-null billing amounts
    where b.monthly_bill_usd is not null
),

-- Aggregate total revenue per age range, plan type, and credit score range
aggregated as (
    select
        age_range,
        plan_type,
        credit_score_range,

        -- Total revenue, rounded to 2 decimal places
        round(sum(monthly_bill_usd), 2) as total_revenue
    from base
    group by age_range, plan_type, credit_score_range
)

-- Final result: customer segmentation by revenue contribution
select *
from aggregated
order by total_revenue desc
