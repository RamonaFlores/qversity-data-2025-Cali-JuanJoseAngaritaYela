{{ config(materialized='table') }}

-- Base CTE: count new customers per month and operator
with base as (
    select
        -- Truncate registration_date to month level
        date_trunc('month', c.registration_date)::date as mes_anio,
        
        -- Mobile service operator
        c.operator,

        -- Count of new customers registered in that month
        count(*) as total_nuevos_clientes

    from {{ ref('customers_cleaned') }} c

    -- Only include customers with valid registration dates
    where c.registration_date between '2000-01-01' and current_date

    group by 1, 2
)

-- Final result: number of new customers by month and operator
select *
from base
order by mes_anio, operator
