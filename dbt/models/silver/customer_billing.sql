{{ config(materialized='table') }}

-- Extract billing and usage metrics from validated customer records
select
    -- Convert customer_id to integer
    (raw::jsonb ->> 'customer_id')::int                as customer_id,

    -- Monthly mobile data allowance (in GB)
    (raw::jsonb ->> 'monthly_data_gb')::float          as monthly_data_gb,

    -- Monthly bill amount in USD (cast to numeric with 2 decimals)
    (raw::jsonb ->> 'monthly_bill_usd')::numeric(12,2) as monthly_bill_usd,

    -- Date of the most recent payment
    (raw::jsonb ->> 'last_payment_date')::date         as last_payment_date,

    -- Credit limit assigned to the customer
    (raw::jsonb ->> 'credit_limit')::numeric(12,2)     as credit_limit,

    -- Actual data usage in the current month (in GB)
    (raw::jsonb ->> 'data_usage_current_month')::float as data_usage_current_month,

    -- Timestamp when the record was ingested into the Bronze layer
    ingestion_timestamp

from {{ source('bronze','customers_raw_json') }}

-- Filter only validated records from the Bronze layer
where record_validated
