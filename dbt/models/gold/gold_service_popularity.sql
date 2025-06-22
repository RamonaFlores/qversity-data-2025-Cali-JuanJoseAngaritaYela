{{ config(
    materialized = 'table'
) }}

with base as (

    select
        service_type
    from {{ ref('customer_services_cleaned') }}
    where service_type is not null

),

counts as (

    select
        service_type,
        count(*) as service_count
    from base
    group by service_type
    order by service_count desc

)

select * from counts
