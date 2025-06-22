{{ config(
    materialized = 'table'
) }}

with base as (

    select
        c.customer_id,
        c.plan_type,
        d.brand
    from {{ ref('customers_cleaned') }} c
    join {{ ref('devices_cleaned') }} d using (customer_id)
    where c.plan_type is not null
      and d.brand is not null

),

device_counts as (

    select
        plan_type,
        brand,
        count(distinct customer_id) as user_count
    from base
    group by plan_type, brand

),

total_counts as (

    select
        plan_type,
        count(distinct customer_id) as total_users
    from base
    group by plan_type

),

final as (

    select
        dc.plan_type,
        dc.brand,
        dc.user_count,
        round(100.0 * dc.user_count / tc.total_users, 2) as percentage
    from device_counts dc
    join total_counts tc
      on dc.plan_type = tc.plan_type

)

select * from final
