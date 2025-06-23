{{ config(materialized='table') }}

-- Base CTE: classify customers into age groups by plan_type
with base as (
    select
        plan_type,

        -- Categorize customers into defined age groups
        case
            when age between 0 and 17 then 'Teen'
            when age between 18 and 25 then 'Young Adult'
            when age between 26 and 45 then 'Adult'
            when age > 45 then 'Senior'
        end as age_group

    from {{ ref('customers_cleaned') }}

    -- Only include customers with valid age
    where age is not null
),

-- Aggregate customer count per plan_type and age group
aggregated as (
    select
        plan_type,
        age_group,
        count(*) as customer_count
    from base
    group by plan_type, age_group
)

-- Final output sorted by plan_type and age_group
select *
from aggregated
order by plan_type, age_group
