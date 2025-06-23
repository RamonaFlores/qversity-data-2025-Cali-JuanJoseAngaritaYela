{{ config(
    materialized = 'table'
) }}

-- Base CTE: extract and normalize service types per customer
with base as (

    select
        customer_id,
        lower(trim(service_type)) as service_type
    from {{ ref('customer_services_cleaned') }}

    -- Exclude null service types
    where service_type is not null

),

-- Aggregate: group services into distinct combinations per customer
aggregated as (

    select
        customer_id,
        -- Create an ordered array of distinct service types per customer
        array_agg(distinct service_type order by service_type) as services_combination
    from base
    group by customer_id

),

-- Count how many customers have each unique service combination
combinations as (

    select
        services_combination,
        count(*) as combination_count
    from aggregated
    group by services_combination
    order by combination_count desc

)

-- Final output: most common service combinations among customers
select * from combinations
