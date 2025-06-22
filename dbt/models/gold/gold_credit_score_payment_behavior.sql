{{ config(materialized='table') }}

with pagos as (
    select
        customer_id,
        count(*) as total_pagos,
        count(*) filter (where lower(status) = 'failed') as pagos_fallidos
    from {{ ref('customer_payments_cleaned') }}
    group by customer_id
),

scoring as (
    select
        customer_id,
        credit_score
    from {{ ref('customers_cleaned') }}
),

combinado as (
    select
        s.customer_id,
        s.credit_score,
        p.total_pagos,
        p.pagos_fallidos,
        round(100.0 * p.pagos_fallidos / nullif(p.total_pagos, 0), 2) as pct_pagos_fallidos
    from scoring s
    left join pagos p using (customer_id)
)

select *
from combinado
where total_pagos > 0
