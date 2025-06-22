{{ config(materialized = 'table') }}

with base as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'device_brand',
            'device_model'
        ]) }} as device_id,
        *
    from {{ ref('devices') }}
),

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

referenced as (
    select d.*
    from deduplicated d
    join {{ ref('customers_cleaned') }} c using (customer_id)
),

limpiado as (
    select
        device_id,
        customer_id,

        -- Normalización de marcas
        case
            when lower(trim(device_brand)) in ('xiaomi', 'xiami', 'xaomi') then 'Xiaomi'
            when lower(trim(device_brand)) in ('apple', 'appl', 'aple') then 'Apple'
            when lower(trim(device_brand)) in ('samsung', 'samsun', 'samsg') then 'Samsung'
            when lower(trim(device_brand)) in ('huawei', 'huwai', 'hauwei') then 'Huawei'
            else initcap(trim(device_brand))
        end as brand,

        -- Normalización del modelo
        'MODEL ' || upper(regexp_replace(trim(device_model), '[^0-9]', '', 'g')) as model,

        ingestion_timestamp

    from referenced
)

select *
from limpiado
where customer_id is not null
