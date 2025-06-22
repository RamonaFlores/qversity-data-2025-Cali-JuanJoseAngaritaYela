{{ config(materialized='table') }}

with base as (
    select
        c.customer_id,
        b.monthly_bill_usd,

        -- Rango de edad
        case
            when c.age < 25 then '<25'
            when c.age between 25 and 40 then '25-40'
            when c.age between 41 and 60 then '41-60'
            else '>60'
        end as age_range,

        -- Plan
        c.plan_type,

        -- Rango de score
        case
            when c.credit_score < 500 then '<500'
            when c.credit_score between 500 and 700 then '500-700'
            when c.credit_score between 701 and 850 then '701-850'
            else '>850'
        end as credit_score_range

    from {{ ref('customers_cleaned') }} c
    join {{ ref('customer_billing_cleaned') }} b
        using (customer_id)
    where b.monthly_bill_usd is not null
),

aggregated as (
    select
        age_range,
        plan_type,
        credit_score_range,
        round(sum(monthly_bill_usd), 2) as total_revenue
    from base
    group by age_range, plan_type, credit_score_range
)

select *
from aggregated
order by total_revenue desc
