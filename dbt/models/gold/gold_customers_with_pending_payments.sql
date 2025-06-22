{{ config(materialized='table') }}

with base as (
    select
        customer_id,
        amount,
        payment_date
    from {{ ref('customer_payments_cleaned') }}
    where lower(status) = 'pending'
      and amount > 0
),

aggregated as (
    select
        customer_id,
        count(*) as pending_payments_count,
        sum(amount) as total_pending_amount,
        max(payment_date) as last_pending_payment_date
    from base
    group by customer_id
)

select *
from aggregated
order by total_pending_amount desc
