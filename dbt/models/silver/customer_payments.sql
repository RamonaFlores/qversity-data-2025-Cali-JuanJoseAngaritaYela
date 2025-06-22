{{ config(materialized='table') }}

with src as (
    select
        raw::jsonb          as raw_json,
        ingestion_timestamp
    from {{ source('bronze','customers_raw_json') }}
    where record_validated
      and jsonb_typeof(raw::jsonb -> 'payment_history') = 'array'
), exploded as (
    select
        (raw_json ->> 'customer_id')::int                     as customer_id,
        p.value                                               as payment_obj,
        ingestion_timestamp
    from src, jsonb_array_elements(raw_json -> 'payment_history') p
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id','payment_obj']) }} as payment_id,
    customer_id,
    (payment_obj ->> 'date')::date                                        as payment_date,
    lower(payment_obj ->> 'status')                                       as status,
    nullif((payment_obj ->> 'amount'),'unknown')::numeric(12,2)           as amount,
    ingestion_timestamp
from exploded
