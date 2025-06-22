{{ config(
    materialized = 'incremental',
    unique_key = 'location_id'
) }}

-- Base dataset: load all or only new records depending on run type
with base as (
    select *
    from {{ ref('customer_location') }}
    {% if is_incremental() %}
    -- For incremental runs, select only new records based on ingestion_timestamp
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

-- Deduplicate based on customer_id + latitude + longitude,
-- keeping the most recent record per combination
deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                 partition by customer_id, latitude, longitude
                 order by ingestion_timestamp desc
               ) rn
        from base
    ) s
    where rn = 1
),

-- Generate a unique surrogate key for each location record
with_location_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'latitude',
            'longitude'
        ]) }} as location_id,
        *
    from deduplicated
),

-- Keep only records with valid customer references (referential integrity)
referenced as (
    select l.*
    from with_location_id l
    join {{ ref('customers_cleaned') }} c using (customer_id)
)

-- Final output with validation rules for latitude and longitude
select
    location_id,
    customer_id,

    -- Latitude must be in valid geographic range [-90, 90]
    case when latitude  between -90  and 90  then latitude  end as latitude,

    -- Longitude must be in valid geographic range [-180, 180]
    case when longitude between -180 and 180 then longitude end as longitude,

    -- Track ingestion time for audit and freshness
    ingestion_timestamp
from referenced
