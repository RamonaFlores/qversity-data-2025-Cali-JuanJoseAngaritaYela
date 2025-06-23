{{ config(
    materialized = 'table'
) }}

-- Base CTE: select unique customer-device brand combinations
with base as (

    select distinct
        customer_id,
        brand
    from {{ ref('devices_cleaned') }}

    -- Only include records with a valid device brand
    where brand is not null

),

-- Count the number of unique users per device brand
counts as (

    select
        brand,
        count(distinct customer_id) as user_count
    from base
    group by brand

    -- Sort brands by user count in descending order
    order by user_count desc

)

-- Final output: device brand distribution by unique users
select * from counts
