{{ config(materialized='table') }}

-- Base CTE: count new customers registered each month
with base as (
    select
        -- Truncate registration date to the month level
        date_trunc('month', registration_date)::date as mes_anio,

        -- Total number of new customers for each month
        count(*) as total_nuevos_clientes
    from {{ ref('customers_cleaned') }}

    -- Only include customers with valid registration dates
    where registration_date between '2000-01-01' and current_date

    group by 1
)

-- Final output: monthly customer registration trends
select *
from base
order by mes_anio
