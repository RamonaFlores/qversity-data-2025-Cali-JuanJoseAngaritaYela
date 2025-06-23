{{ config(
    materialized = 'table'
) }}

-- Base CTE: select valid service types
with base as (

    select
        service_type
    from {{ ref('customer_services_cleaned') }}

    -- Exclude records with null service type
    where service_type is not null

),

-- Count the number of occurrences of each service type
counts as (

    select
        service_type,
        count(*) as service_count
    from base
    group by service_type
    order by service_count desc

)

-- Final output: frequency of each service type
select * from counts
