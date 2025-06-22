{{ config(
    materialized = 'table'
) }}

with services as (

    select
        customer_id,
        lower(trim(service_type)) as service_type
    from {{ ref('customer_services_cleaned') }}
    where service_type is not null

),

aggregated_services as (

    select
        customer_id,
        array_agg(distinct service_type order by service_type) as services_combination
    from services
    group by customer_id

),

billing as (

    select
        customer_id,
        max(monthly_bill_usd) as monthly_bill_usd
    from {{ ref('customer_billing_cleaned') }}
    group by customer_id

),

joined as (

    select
        s.services_combination,
        b.monthly_bill_usd
    from aggregated_services s
    join billing b using (customer_id)
    where b.monthly_bill_usd is not null

),

revenue_by_combo as (

    select
        services_combination,
        sum(monthly_bill_usd) as total_revenue,
        avg(monthly_bill_usd) as avg_revenue,
        count(*) as customers
    from joined
    group by services_combination
    order by total_revenue desc

)

select * from revenue_by_combo
