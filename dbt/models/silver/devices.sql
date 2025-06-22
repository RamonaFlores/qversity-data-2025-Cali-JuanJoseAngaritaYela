{{ config(materialized='table') }}

select
    (raw::jsonb ->> 'customer_id')::int  as customer_id,
    initcap(raw::jsonb ->> 'device_brand')  as device_brand,
    raw::jsonb ->> 'device_model'           as device_model,
    ingestion_timestamp
from {{ source('bronze','customers_raw_json') }}
where record_validated
