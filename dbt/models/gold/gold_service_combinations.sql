{{ config(
    materialized = 'table'
) }}

with base as (

    select
        customer_id,
        lower(trim(service_type)) as service_type
    from {{ ref('customer_services_cleaned') }}
    where service_type is not null

),

-- Agrupar por cliente y juntar sus servicios
aggregated as (

    select
        customer_id,
        array_agg(distinct service_type order by service_type) as services_combination
    from base
    group by customer_id

),

-- Contar frecuencia de cada combinación
combinations as (

    select
        services_combination,
        count(*) as combination_count
    from aggregated
    group by services_combination
    order by combination_count desc

)

select * from combinations
