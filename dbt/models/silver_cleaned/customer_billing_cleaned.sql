{{ config(
    materialized = 'incremental',
    unique_key = 'billing_id'
) }}

-- Base dataset: includes new records only if in incremental mode
with base as (
    select *
    from {{ ref('customer_billing') }}
    {% if is_incremental() %}
    -- In incremental runs, select only records with newer ingestion timestamps
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

-- Deduplicate entries by keeping the most recent per customer and billing details
deduplicated as (
    select *,
           row_number() over (
               partition by customer_id, last_payment_date, monthly_bill_usd
               order by ingestion_timestamp desc
           ) as rn
    from base
),

-- Filter to keep only the latest record per customer-billing-date combination
filtered as (
    select *
    from deduplicated
    where rn = 1
),

-- Generate a unique billing_id using a hash of customer_id, last_payment_date, and amount
with_billing_id as (
    select
        md5(
            concat_ws(
                '|',
                coalesce(cast(customer_id as text), ''),
                coalesce(cast(last_payment_date as text), ''),
                coalesce(cast(monthly_bill_usd as text), '')
            )
        ) as billing_id,
        *
    from filtered
),

-- Ensure referential integrity: only include records with valid customer_id in customers_cleaned
referenced as (
    select b.*
    from with_billing_id b
    join {{ ref('customers_cleaned') }} c using (customer_id)
)

-- Final output with validation and corrections applied
select
    billing_id,
    customer_id,

    -- Ensure date is within reasonable range
    case when last_payment_date BETWEEN '2000-01-01' AND current_date
         then last_payment_date end as last_payment_date,

    -- Validate billing amount range
    case when monthly_bill_usd >= 0 and monthly_bill_usd < 100000
         then monthly_bill_usd end as monthly_bill_usd,

    -- Validate credit limit range
    case when credit_limit >= 0 and credit_limit < 1000000
         then credit_limit end as credit_limit,

    -- Ensure data usage is non-negative
    case when data_usage_current_month >= 0
         then data_usage_current_month end as data_usage_current_month,

    -- Normalize negative monthly data values to 0
    case when monthly_data_gb < 0
         then 0
         else monthly_data_gb end as monthly_data_gb,

    -- Track ingestion time for lineage and audit
    ingestion_timestamp
from referenced
