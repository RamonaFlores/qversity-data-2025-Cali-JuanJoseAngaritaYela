{{ config(materialized='table') }}

-- Aggregate payments: total and failed payments per customer
with pagos as (
    select
        customer_id,
        count(*) as total_pagos,

        -- Count only failed payments using a filter
        count(*) filter (where lower(status) = 'failed') as pagos_fallidos
    from {{ ref('customer_payments_cleaned') }}
    group by customer_id
),

-- Extract credit scores from cleaned customer data
scoring as (
    select
        customer_id,
        credit_score
    from {{ ref('customers_cleaned') }}
),

-- Combine credit score and payment behavior, and compute failure percentage
combinado as (
    select
        s.customer_id,
        s.credit_score,
        p.total_pagos,
        p.pagos_fallidos,

        -- Calculate percentage of failed payments (avoid division by zero)
        round(100.0 * p.pagos_fallidos / nullif(p.total_pagos, 0), 2) as pct_pagos_fallidos
    from scoring s
    left join pagos p using (customer_id)
)

-- Final result: only include customers with at least one payment
select *
from combinado
where total_pagos > 0
