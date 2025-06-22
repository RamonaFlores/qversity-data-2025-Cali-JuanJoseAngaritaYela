{{ config(materialized='table') }}

select
    (raw::jsonb ->> 'customer_id')::int   as customer_id,
    (raw::jsonb ->> 'latitude')::float    as latitude,
    (raw::jsonb ->> 'longitude')::float   as longitude,
    ingestion_timestamp
from {{ source('bronze','customers_raw_json') }}
where record_validated
