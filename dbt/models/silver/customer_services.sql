{{ config(materialized='table') }}

-- Extract and normalize contracted services from validated JSON records
with src as (
    -- Load validated records from Bronze layer and parse JSON
    select raw::jsonb as raw_json, ingestion_timestamp
    from {{ source('bronze','customers_raw_json') }}
    where record_validated
), unnested as (
    -- Unnest the 'contracted_services' array from each JSON record
    select
        (raw_json ->> 'customer_id')::int as customer_id,  -- Cast customer_id to integer
        lower(s.value)::text              as service,       -- Normalize service name to lowercase
        ingestion_timestamp                                 -- Preserve ingestion timestamp
    from src, jsonb_array_elements_text(raw_json -> 'contracted_services') s
)

-- Remove duplicates in case of redundant entries
select distinct * from unnested
