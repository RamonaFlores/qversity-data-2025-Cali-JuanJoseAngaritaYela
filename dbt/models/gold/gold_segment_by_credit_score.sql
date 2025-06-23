{{ config(materialized='table') }}

-- Base CTE: classify customers into credit score segments
with base as (
    select
        customer_id,
        credit_score,

        -- Segment credit scores into categories
        case
            when credit_score between 300 and 499 then 'Poor'
            when credit_score between 500 and 599 then 'Fair'
            when credit_score between 600 and 699 then 'Good'
            when credit_score between 700 and 749 then 'Very Good'
            when credit_score between 750 and 850 then 'Excellent'
            else 'Unknown'
        end as credit_segment

    from {{ ref('customers_cleaned') }}
),

-- Group and count customers per credit score segment
grouped as (
    select
        credit_segment,
        count(*) as customer_count
    from base
    group by 1
)

-- Final output: distribution of customers by credit segment
select *
from grouped
order by customer_count desc
