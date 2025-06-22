{{ config(
    materialized = 'incremental',
    unique_key = 'service_id'
) }}

with base as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'service',
            'ingestion_timestamp'
        ]) }} as service_id,
        *
    from {{ ref('customer_services') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

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

referenced as (
    select s.*
    from deduplicated s
    join {{ ref('customers_cleaned') }} c using (customer_id)
),

cleaned as (
    select
        service_id,
        customer_id,

        case
            when lower(service) in ('data','voice','sms','tv','bundle')
            then lower(service)
        end as service_type,

        ingestion_timestamp
    from referenced
    where lower(service) in ('data','voice','sms','tv','bundle')
)

select *
from cleaned
