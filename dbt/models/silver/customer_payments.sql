{{ config(materialized='table') }}

-- Extract and normalize payment history from validated customer records
with src as (
    -- Load validated records where 'payment_history' is a JSON array
    select
        raw::jsonb          as raw_json,
        ingestion_timestamp
    from {{ source('bronze','customers_raw_json') }}
    where record_validated
      and jsonb_typeof(raw::jsonb -> 'payment_history') = 'array'
), exploded as (
    -- Unnest each element of the 'payment_history' array
    select
        (raw_json ->> 'customer_id')::int                     as customer_id,
        p.value                                               as payment_obj,  -- full JSON object for a single payment
        ingestion_timestamp
    from src, jsonb_array_elements(raw_json -> 'payment_history') p
)

-- Transform and clean payment records
select
    {{ dbt_utils.generate_surrogate_key(['customer_id','payment_obj']) }} as payment_id,  -- unique key for each payment
    customer_id,
    (payment_obj ->> 'date')::date                                        as payment_date,  -- parse date string
    lower(payment_obj ->> 'status')                                       as status,        -- normalize status to lowercase
    nullif((payment_obj ->> 'amount'),'unknown')::numeric(12,2)           as amount,        -- convert amount to number, treat "unknown" as null
    ingestion_timestamp
from exploded
