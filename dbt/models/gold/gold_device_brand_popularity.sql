{{ config(
    materialized = 'table'
) }}

with base as (

    select distinct
        customer_id,
        brand
    from {{ ref('devices_cleaned') }}
    where brand is not null

),

counts as (

    select
        brand,
        count(distinct customer_id) as user_count
    from base
    group by brand
    order by user_count desc

)

select * from counts
