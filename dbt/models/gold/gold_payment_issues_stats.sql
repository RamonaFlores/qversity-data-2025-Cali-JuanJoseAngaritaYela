{{ config(materialized='table') }}

-- Extract relevant payment fields
with payments as (
    select customer_id, status, amount
    from {{ ref('customer_payments_cleaned') }}
),

-- Identify customers with payment issues: either failed status or missing amount
customers_with_issues as (
    select distinct customer_id
    from payments
    where lower(status) = 'failed'
       or amount is null
),

-- Get the total number of distinct customers in the dataset
total_customers as (
    select count(distinct customer_id) as total_customers
    from {{ ref('customers_cleaned') }}
),

-- Calculate number and percentage of customers with payment issues
issue_stats as (
    select
        -- Total number of customers with issues
        (select count(*) from customers_with_issues) as customers_with_issues,

        -- Total number of customers in the dataset
        (select total_customers from total_customers) as total_customers,

        -- Percentage of affected customers
        round(
            100.0 * (select count(*) from customers_with_issues) 
            / nullif((select total_customers from total_customers), 0), 
        2) as percentage_with_issues
)

-- Final result: overall payment issue statistics
select * from issue_stats
