{{ config(materialized='table') }}

with payments as (
    select customer_id, status, amount
    from {{ ref('customer_payments_cleaned') }}
),

customers_with_issues as (
    select distinct customer_id
    from payments
    where lower(status) = 'failed'
       or amount is null
),

total_customers as (
    select count(distinct customer_id) as total_customers
    from {{ ref('customers_cleaned') }}
),

issue_stats as (
    select
        (select count(*) from customers_with_issues) as customers_with_issues,
        (select total_customers from total_customers) as total_customers,
        round(100.0 * (select count(*) from customers_with_issues) / nullif((select total_customers from total_customers), 0), 2) as percentage_with_issues
)

select * from issue_stats
