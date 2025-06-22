{{ config(
    materialized = 'incremental',
    unique_key = 'location_id'
) }}

with base as (
    select *
    from {{ ref('customer_location') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

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

referenced as (
    select l.*
    from with_location_id l
    join {{ ref('customers_cleaned') }} c using (customer_id)
)

select
    location_id,
    customer_id,

    case when latitude  between -90  and 90  then latitude  end as latitude,
    case when longitude between -180 and 180 then longitude end as longitude,

    ingestion_timestamp
from referenced
