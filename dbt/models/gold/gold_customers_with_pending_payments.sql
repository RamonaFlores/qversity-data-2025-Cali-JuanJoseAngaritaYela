{{ config(materialized='table') }}

-- Base CTE: filter pending payments with valid positive amounts
with base as (
    select
        customer_id,
        amount,
        payment_date
    from {{ ref('customer_payments_cleaned') }}

    -- Only include payments with status = 'pending' and amount > 0
    where lower(status) = 'pending'
      and amount > 0
),

-- Aggregate pending payments per customer
aggregated as (
    select
        customer_id,

        -- Number of pending payments
        count(*) as pending_payments_count,

        -- Total pending amount per customer
        sum(amount) as total_pending_amount,

        -- Most recent pending payment date
        max(payment_date) as last_pending_payment_date
    from base
    group by customer_id
)

-- Final result: customers with pending payments, ordered by amount
select *
from aggregated
order by total_pending_amount desc
