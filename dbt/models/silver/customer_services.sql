{{ config(materialized='table') }}

with src as (
    select raw::jsonb as raw_json, ingestion_timestamp
    from {{ source('bronze','customers_raw_json') }}
    where record_validated
), unnested as (
    select
        (raw_json ->> 'customer_id')::int as customer_id,
        lower(s.value)::text              as service,
        ingestion_timestamp
    from src, jsonb_array_elements_text(raw_json -> 'contracted_services') s
)

select distinct * from unnested
