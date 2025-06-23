{{ config(
    materialized = 'table'
) }}

-- Step 1: Extract cleaned service types per customer
with services as (

    select
        customer_id,
        lower(trim(service_type)) as service_type
    from {{ ref('customer_services_cleaned') }}

    -- Exclude records with null service types
    where service_type is not null

),

-- Step 2: Aggregate distinct service types into an array per customer
aggregated_services as (

    select
        customer_id,
        array_agg(distinct service_type order by service_type) as services_combination
    from services
    group by customer_id

),

-- Step 3: Get maximum monthly bill per customer
billing as (

    select
        customer_id,
        max(monthly_bill_usd) as monthly_bill_usd
    from {{ ref('customer_billing_cleaned') }}
    group by customer_id

),

-- Step 4: Join services with billing, keeping only customers with valid billing info
joined as (

    select
        s.services_combination,
        b.monthly_bill_usd
    from aggregated_services s
    join billing b using (customer_id)
    where b.monthly_bill_usd is not null

),

-- Step 5: Aggregate revenue metrics by service combination
revenue_by_combo as (

    select
        services_combination,

        -- Total revenue for each combination
        sum(monthly_bill_usd) as total_revenue,

        -- Average revenue per customer for each combination
        avg(monthly_bill_usd) as avg_revenue,

        -- Number of customers in each combination group
        count(*) as customers
    from joined
    group by services_combination
    order by total_revenue desc

)

-- Final output: revenue insights by service combination
select * from revenue_by_combo
