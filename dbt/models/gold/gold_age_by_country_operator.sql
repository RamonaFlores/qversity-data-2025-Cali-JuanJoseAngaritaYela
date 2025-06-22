{{ config(materialized='table') }}

with base as (
    select
        country,
        operator,
        case
            when age between 0 and 17 then 'Teen'
            when age between 18 and 25 then 'Young Adult'
            when age between 26 and 45 then 'Adult'
            when age > 45 then 'Senior'
        end as age_group
    from {{ ref('customers_cleaned') }}
    where age is not null
),

aggregated as (
    select
        country,
        operator,
        age_group,
        count(*) as customer_count
    from base
    group by country, operator, age_group
)

select *
from aggregated
order by country, operator, age_group
