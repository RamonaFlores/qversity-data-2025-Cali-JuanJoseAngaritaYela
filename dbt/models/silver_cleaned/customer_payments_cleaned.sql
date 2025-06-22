{{ config(
    materialized = 'incremental',
    unique_key = 'payment_id'
) }}

-- Base dataset: includes all records or only new ones if incremental
with base as (
    select *
    from {{ ref('customer_payments') }}
    {% if is_incremental() %}
    -- For incremental runs, only select records newer than the latest ingestion
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

-- Deduplicate payments by payment_id, keeping the latest by ingestion_timestamp
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

-- Enforce referential integrity with cleaned customer dimension
referenced as (
    select p.*
    from deduplicated p
    join {{ ref('customers_cleaned') }} c using (customer_id)
),

-- Clean and validate fields according to business rules
cleaned as (
    select
        payment_id,
        customer_id,

        -- Keep payment dates only if within a valid historical range
        case
            when payment_date BETWEEN '2000-01-01' AND current_date
            then payment_date
        end as payment_date,

        -- Accept amounts in a valid business range (>0 and <10 million)
        case
            when amount > 0 and amount < 10000000
            then amount
        end as amount,

        -- Normalize status and only allow accepted values
        case
            when lower(trim(status)) in ('paid','pending','failed')
            then lower(trim(status))
        end as status,

        -- Preserve ingestion timestamp for lineage
        ingestion_timestamp
    from referenced

    -- Exclude invalid records: missing ID, invalid status, or missing amount for 'paid'
    where
        payment_id is not null
        and status is not null
        and not (status = 'paid' and amount is null)
)

-- Final result set to be materialized
select *
from cleaned
