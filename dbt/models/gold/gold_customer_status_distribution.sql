{{ config(materialized='table') }}

with total_customers as (
    select count(*) as total
    from {{ ref('customers_cleaned') }}
    where lower(status) in ('active', 'suspended', 'inactive')
),

status_distribution as (
    select
        lower(status) as customer_status,
        count(*) as count_status
    from {{ ref('customers_cleaned') }}
    where lower(status) in ('active', 'suspended', 'inactive')
    group by 1
)

select
    sd.customer_status,
    sd.count_status,
    round(100.0 * sd.count_status / tc.total, 2) as percentage
from status_distribution sd
cross join total_customers tc
order by percentage desc
