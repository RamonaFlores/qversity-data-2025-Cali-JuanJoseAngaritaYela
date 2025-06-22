{{ config(materialized='table') }}

with src as (

    -- 1 Select only valid rows (record_validated = true)
    select
        raw::jsonb                                    as raw_json,
        ingestion_timestamp
    from {{ source('bronze','customers_raw_json') }}
    where record_validated

), filtered as (

    --  1.1 Validate that JSON contains required fields and correct types
    select *
    from src
    where
        jsonb_typeof(raw_json) = 'object'                            -- must be a JSON object
        and (raw_json ? 'customer_id')                               -- must contain customer_id
        and (raw_json ? 'age')                                       -- must contain age
        and jsonb_typeof(raw_json -> 'age') in ('string', 'number') -- age must be a number or numeric string
        and (raw_json ->> 'customer_id') ~ '^\d+$'                   -- customer_id must be numeric

), flattened as (

    -- 2 Flatten top-level JSON fields -----------------------------------
    select
        (raw_json ->> 'customer_id')::int                          as customer_id,
        initcap(raw_json ->> 'first_name')                         as first_name,
        initcap(raw_json ->> 'last_name')                          as last_name,
        lower(raw_json ->> 'email')                                as email,
        raw_json ->> 'phone_number'                                as phone_number,
        cast(nullif(raw_json ->> 'age', '') as float)::int         as age,
        raw_json ->> 'country'                                     as country_raw,
        raw_json ->> 'city'                                        as city_raw,
        raw_json ->> 'operator'                                    as operator_raw,
        raw_json ->> 'plan_type'                                   as plan_type_raw,

        -- Robust handling of date formats -----------------------------
        case
            when (raw_json ->> 'registration_date') ~ '^\d{4}-\d{2}-\d{2}$' then
                (raw_json ->> 'registration_date')::date
            when (raw_json ->> 'registration_date') ~ '^\d{2}-\d{2}-\d{2}$' then
                to_date(raw_json ->> 'registration_date', 'DD-MM-YY')
            when (raw_json ->> 'registration_date') ~ '^\d{2}/\d{2}/\d{4}$' then
                to_date(raw_json ->> 'registration_date', 'DD/MM/YYYY')
            else null
        end                                                      as registration_date,

        raw_json ->> 'status'                                      as status_raw,
        cast(nullif(raw_json ->> 'credit_score', '') as float)::int as credit_score,
        ingestion_timestamp
    from filtered

), standardized as (

    -- 3 Normalize values using seed mapping dictionaries ----------------
    select
        f.customer_id,
        f.first_name,
        f.last_name,
        f.email,
        f.phone_number,
        f.age,

        coalesce(map_country.standard,  initcap(f.country_raw))   as country,
        initcap(f.city_raw)                                       as city,
        coalesce(map_operator.standard, initcap(f.operator_raw))  as operator,
        coalesce(map_plan.standard,     initcap(f.plan_type_raw)) as plan_type,
        f.registration_date,
        coalesce(map_status.standard,   lower(f.status_raw))      as status,
        f.credit_score,
        f.ingestion_timestamp
    from flattened f
    left join {{ ref('mapping_dictionaries') }} map_country
           on map_country.domain = 'country'  and map_country.raw = f.country_raw
    left join {{ ref('mapping_dictionaries') }} map_operator
           on map_operator.domain = 'operator' and map_operator.raw = f.operator_raw
    left join {{ ref('mapping_dictionaries') }} map_plan
           on map_plan.domain = 'plan_type'    and map_plan.raw = f.plan_type_raw
    left join {{ ref('mapping_dictionaries') }} map_status
           on map_status.domain = 'status'     and map_status.raw = f.status_raw

)

-- Final SELECT: returns cleaned and standardized customer data
select * from standardized
