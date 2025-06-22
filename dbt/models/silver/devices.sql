{{ config(materialized='table') }}

-- Extracts validated device information from raw JSON records
select
    -- Cast customer_id to integer
    (raw::jsonb ->> 'customer_id')::int           as customer_id,

    -- Capitalize brand name (e.g., "samsung" → "Samsung")
    initcap(raw::jsonb ->> 'device_brand')        as device_brand,

    -- Keep device_model as-is (can be null or string)
    raw::jsonb ->> 'device_model'                 as device_model,

    -- Include original ingestion timestamp from Bronze layer
    ingestion_timestamp

from {{ source('bronze','customers_raw_json') }}

-- Only use records that passed validation checks in Bronze layer
where record_validated
