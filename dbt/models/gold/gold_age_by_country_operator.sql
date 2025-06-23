{{ config(materialized='table') }}

-- Base CTE: classify customers into age groups
with base as (
    select
        country,
        operator,

        -- Assign age group based on customer's age
        case
            when age between 0 and 17 then 'Teen'
            when age between 18 and 25 then 'Young Adult'
            when age between 26 and 45 then 'Adult'
            when age > 45 then 'Senior'
        end as age_group

    from {{ ref('customers_cleaned') }}

    -- Exclude records with null age
    where age is not null
),

-- Aggregate number of customers per country, operator, and age group
aggregated as (
    select
        country,
        operator,
        age_group,
        count(*) as customer_count
    from base
    group by country, operator, age_group
)

-- Final output: sorted for readability
select *
from aggregated
order by country, operator, age_group
