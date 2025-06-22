{{ config(materialized='table') }}

select
    country,
    city,
    count(customer_id) as customer_count
from {{ ref('customers_cleaned') }}
where country is not null and city is not null
group by country, city
order by customer_count desc
