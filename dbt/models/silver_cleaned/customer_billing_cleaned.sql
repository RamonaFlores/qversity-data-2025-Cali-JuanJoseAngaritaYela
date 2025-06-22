{{ config(
    materialized = 'incremental',
    unique_key = 'billing_id'
) }}

with base as (
    select *
    from {{ ref('customer_billing') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

deduplicated as (
    select *,
           row_number() over (
               partition by customer_id, last_payment_date, monthly_bill_usd
               order by ingestion_timestamp desc
           ) as rn
    from base
),

filtered as (
    select *
    from deduplicated
    where rn = 1
),

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

referenced as (
    select b.*
    from with_billing_id b
    join {{ ref('customers_cleaned') }} c using (customer_id)
)

select
    billing_id,
    customer_id,

    case when last_payment_date BETWEEN '2000-01-01' AND current_date
         then last_payment_date end as last_payment_date,

    case when monthly_bill_usd >= 0 and monthly_bill_usd < 100000
         then monthly_bill_usd end as monthly_bill_usd,

    case when credit_limit >= 0 and credit_limit < 1000000
         then credit_limit end as credit_limit,

    case when data_usage_current_month >= 0
         then data_usage_current_month end as data_usage_current_month,

    monthly_data_gb,
    ingestion_timestamp
from referenced
