{{ config(materialized='table') }}

-- Count customers by country and city
select
    country,
    city,

    -- Total number of customers in each location
    count(customer_id) as customer_count

from {{ ref('customers_cleaned') }}

-- Only include records with both country and city present
where country is not null and city is not null

group by country, city

-- Sort by descending customer count
order by customer_count desc
