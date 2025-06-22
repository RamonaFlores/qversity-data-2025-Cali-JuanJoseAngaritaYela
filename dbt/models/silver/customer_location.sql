{{ config(materialized='table') }}

-- Extract geolocation data from validated JSON records
select
    -- Cast customer_id to integer
    (raw::jsonb ->> 'customer_id')::int   as customer_id,

    -- Convert latitude and longitude to float
    (raw::jsonb ->> 'latitude')::float    as latitude,
    (raw::jsonb ->> 'longitude')::float   as longitude,

    -- Include ingestion timestamp from Bronze layer
    ingestion_timestamp

from {{ source('bronze','customers_raw_json') }}

-- Only include records that passed validation in the Bronze layer
where record_validated
