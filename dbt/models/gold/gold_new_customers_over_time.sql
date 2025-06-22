{{ config(materialized='table') }}

with base as (
    select
        date_trunc('month', registration_date)::date as mes_anio,
        count(*) as total_nuevos_clientes
    from {{ ref('customers_cleaned') }}
    where registration_date between '2000-01-01' and current_date
    group by 1
)

select *
from base
order by mes_anio
