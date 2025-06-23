{{ config(
    materialized = 'table'
) }}

-- Base CTE: join customers with their associated device brands
with base as (

    select
        c.customer_id,
        c.country,
        c.operator,
        d.brand
    from {{ ref('customers_cleaned') }} c
    join {{ ref('devices_cleaned') }} d using (customer_id)

    -- Filter out records with null country, operator, or brand
    where c.country is not null
      and c.operator is not null
      and d.brand is not null

),

-- Count number of distinct users per brand, country, and operator
device_counts as (

    select
        country,
        operator,
        brand,
        count(distinct customer_id) as user_count
    from base
    group by country, operator, brand

),

-- Count total number of distinct users per country and operator
total_counts as (

    select
        country,
        operator,
        count(distinct customer_id) as total_users
    from base
    group by country, operator

),

-- Final output: calculate percentage share of each device brand per country/operator
final as (

    select
        dc.country,
        dc.operator,
        dc.brand,
        dc.user_count,

        -- Compute percentage of users using each brand
        round(100.0 * dc.user_count / tc.total_users, 2) as percentage
    from device_counts dc
    join total_counts tc
      on dc.country = tc.country
     and dc.operator = tc.operator

)

-- Final result with device usage distribution by geography and operator
select * from final
