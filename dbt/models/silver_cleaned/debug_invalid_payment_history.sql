{{ config(materialized='table') }}

select
    (raw::jsonb ->> 'customer_id')::int as customer_id,
    raw::jsonb -> 'payment_history' as payment_history_raw,
    ingestion_timestamp
from {{ source('bronze','customers_raw_json') }}
where record_validated
  and jsonb_typeof(raw::jsonb -> 'payment_history') != 'array'
