{{ config(materialized = 'table') }}

-- Base dataset with surrogate key generation for device records
with base as (
    select
        -- Generate unique device_id using customer_id, brand, and model
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'device_brand',
            'device_model'
        ]) }} as device_id,
        *
    from {{ ref('devices') }}
),

-- Deduplicate devices: keep the most recent record per device_id
deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by device_id
                   order by ingestion_timestamp desc
               ) as rn
        from base
    ) s
    where rn = 1
),

-- Enforce referential integrity: only devices linked to cleaned customers
referenced as (
    select d.*
    from deduplicated d
    join {{ ref('customers_cleaned') }} c using (customer_id)
),

-- Clean and normalize brand and model values
limpiado as (
    select
        device_id,
        customer_id,

        --  Normalize device brand using common variants
        case
            when lower(trim(device_brand)) in ('xiaomi', 'xiami', 'xaomi') then 'Xiaomi'
            when lower(trim(device_brand)) in ('apple', 'appl', 'aple') then 'Apple'
            when lower(trim(device_brand)) in ('samsung', 'samsun', 'samsg') then 'Samsung'
            when lower(trim(device_brand)) in ('huawei', 'huwai', 'hauwei') then 'Huawei'
            else initcap(trim(device_brand))
        end as brand,

        -- Normalize device model: remove all non-numeric characters and prefix with "MODEL"
        'MODEL ' || upper(regexp_replace(trim(device_model), '[^0-9]', '', 'g')) as model,

        --  Preserve ingestion timestamp
        ingestion_timestamp

    from referenced
)

--  Final selection: filter out records with null customer_id
select *
from limpiado
where customer_id is not null
