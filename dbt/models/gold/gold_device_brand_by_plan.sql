{{ config(
    materialized = 'table'
) }}

-- Base CTE: join customers with their associated devices
with base as (

    select
        c.customer_id,
        c.plan_type,
        d.brand
    from {{ ref('customers_cleaned') }} c
    join {{ ref('devices_cleaned') }} d using (customer_id)

    -- Only include records with non-null plan type and brand
    where c.plan_type is not null
      and d.brand is not null

),

-- Count number of unique users by plan type and device brand
device_counts as (

    select
        plan_type,
        brand,
        count(distinct customer_id) as user_count
    from base
    group by plan_type, brand

),

-- Count total number of users per plan type
total_counts as (

    select
        plan_type,
        count(distinct customer_id) as total_users
    from base
    group by plan_type

),

-- Final CTE: calculate percentage of users by device brand within each plan type
final as (

    select
        dc.plan_type,
        dc.brand,
        dc.user_count,

        -- Compute brand share percentage within each plan type
        round(100.0 * dc.user_count / tc.total_users, 2) as percentage
    from device_counts dc
    join total_counts tc
      on dc.plan_type = tc.plan_type

)

-- Final output: brand distribution per plan type
select * from final
