{{ config(
    materialized = 'incremental',
    unique_key = 'payment_id'
) }}

with base as (
    select *
    from {{ ref('customer_payments') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                 partition by payment_id
                 order by ingestion_timestamp desc
               ) rn
        from base
    ) s
    where rn = 1
),

referenced as (
    select p.*
    from deduplicated p
    join {{ ref('customers_cleaned') }} c using (customer_id)
),

cleaned as (
    select
        payment_id,
        customer_id,

        case
            when payment_date BETWEEN '2000-01-01' AND current_date
            then payment_date
        end as payment_date,

        case
            when amount > 0 and amount < 10000000
            then amount
        end as amount,

        case
            when lower(trim(status)) in ('paid','pending','failed')
            then lower(trim(status))
        end as status,

        ingestion_timestamp
    from referenced
    where
        payment_id is not null
        and status is not null
        and not (status = 'paid' and amount is null)
)

select *
from cleaned
