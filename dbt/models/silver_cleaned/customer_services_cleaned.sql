{{ config(
    materialized = 'incremental',
    unique_key = 'service_id'
) }}

-- Base dataset: generate service_id and optionally filter for incremental runs
with base as (
    select
        -- Generate a surrogate key based on customer_id, service, and ingestion timestamp
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'service',
            'ingestion_timestamp'
        ]) }} as service_id,
        *
    from {{ ref('customer_services') }}
    {% if is_incremental() %}
    -- If incremental run, select only records with newer ingestion_timestamp
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

-- Deduplicate by service_id, keeping the latest record based on ingestion timestamp
deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by service_id
                   order by ingestion_timestamp desc
               ) rn
        from base
    ) s
    where rn = 1
),

-- Ensure referential integrity by joining with cleaned customers
referenced as (
    select s.*
    from deduplicated s
    join {{ ref('customers_cleaned') }} c using (customer_id)
),

-- Clean and validate service types against allowed values
cleaned as (
    select
        service_id,
        customer_id,

        -- Normalize and filter only accepted service types
        case
            when lower(service) in ('data','voice','sms','tv','bundle')
            then lower(service)
        end as service_type,

        -- Preserve ingestion timestamp
        ingestion_timestamp
    from referenced

    -- Filter again to exclude unexpected service types
    where lower(service) in ('data','voice','sms','tv','bundle')
)

-- Final result set to be materialized
select *
from cleaned
