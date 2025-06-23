{{ config(materialized='table') }}

-- CTE: Get the total number of customers with a recognized status
with total_customers as (
    select count(*) as total
    from {{ ref('customers_cleaned') }}
    where lower(status) in ('active', 'suspended', 'inactive')
),

-- CTE: Count how many customers fall into each valid status category
status_distribution as (
    select
        lower(status) as customer_status,  -- Normalize status to lowercase
        count(*) as count_status           -- Total count per status
    from {{ ref('customers_cleaned') }}
    where lower(status) in ('active', 'suspended', 'inactive')
    group by 1
)

-- Final result: status counts and percentage of total
select
    sd.customer_status,
    sd.count_status,
    round(100.0 * sd.count_status / tc.total, 2) as percentage
from status_distribution sd
cross join total_customers tc
order by percentage desc
