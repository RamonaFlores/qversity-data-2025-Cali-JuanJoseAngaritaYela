{{ config(materialized='table') }}

select
    (raw::jsonb ->> 'customer_id')::int                as customer_id,
    (raw::jsonb ->> 'monthly_data_gb')::float          as monthly_data_gb,
    (raw::jsonb ->> 'monthly_bill_usd')::numeric(12,2) as monthly_bill_usd,
    (raw::jsonb ->> 'last_payment_date')::date         as last_payment_date,
    (raw::jsonb ->> 'credit_limit')::numeric(12,2)     as credit_limit,
    (raw::jsonb ->> 'data_usage_current_month')::float as data_usage_current_month,
    ingestion_timestamp
from {{ source('bronze','customers_raw_json') }}
where record_validated
