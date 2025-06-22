{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('customers') }}

), deduplicated as (

    -- 1️⃣ Elimina duplicados por e-mail *y* por customer_id
    select *
    from (
        select *,
               row_number() over (
                   partition by customer_id, lower(trim(email))
                   order by ingestion_timestamp desc
               ) as rn
        from base
    ) x
    where rn = 1

), limpiado as (

    select
        customer_id,

        -- Limpieza de first_name
        case
            when lower(first_name) in ('lau', 'laur') then 'Laura'
            when lower(first_name) in ('an') then 'Ana'
            when lower(first_name) in ('carme', 'carm', 'carmen') then 'Carmen'
            when lower(first_name) in ('pedr', 'pedro') then 'Pedro'
            when lower(first_name) in ('mig', 'miguel') then 'Miguel'
            when lower(first_name) in ('mara', 'maria', 'maría', 'mari 3') then 'María'
            when lower(first_name) in ('sofi', 'sofia') then 'Sofía'
            when lower(first_name) in ('jua', 'juan', 'jun') then 'Juan'
            when lower(first_name) in ('carls', 'carlos') then 'Carlos'
            when lower(first_name) in ('lui', 'luis') then 'Luis'
            else initcap(trim(first_name))
        end                                                         as first_name,

        -- Limpieza de last_name
        case
            when lower(last_name) in ('rodrigue', 'rodríguez', 'rodriguez') then 'Rodríguez'
            when lower(last_name) in ('fernandez', 'fernández', 'fernan') then 'Fernández'
            when lower(last_name) in ('lopez', 'lópez', 'lope', 'lópe') then 'López'
            when lower(last_name) in ('garcía', 'garci', 'garcí', 'garcia') then 'García'
            when lower(last_name) in ('gonzalez', 'gonzále', 'gonzale', 'gonzález') then 'González'
            when lower(last_name) in ('martinez', 'martíne', 'martin') then 'Martínez'
            else initcap(trim(last_name))
        end                                                         as last_name,

        case
            when email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            then lower(trim(email))
        end                                                         as email,

        case
            when regexp_replace(phone_number, '\D', '', 'g') ~ '^\d{7,15}$'
            then phone_number
        end                                                         as phone_number,

        case
            when age between 0 and 120 then age
        end                                                         as age,

        -- Limpieza completa de country
        case
            when lower(trim(country)) in (
                'colombia', 'col', 'colomia', 'co'
            ) then 'Colombia'

            when lower(trim(country)) in (
                'mexico', 'méxico', 'mex', 'mejico', 'mx', 'mexco'
            ) then 'México'

            when lower(trim(country)) in (
                'perú', 'peru', 'per', 'pe', 'pru'
            ) then 'Perú'

            when lower(trim(country)) in (
                'argentina', 'argentin', 'arg', 'ar'
            ) then 'Argentina'

            when lower(trim(country)) in (
                'chile', 'chi', 'chl', 'chle'
            ) then 'Chile'

            else initcap(trim(country))
        end                                                         as country,

        -- Limpieza de city
        initcap(trim(
            case
                when lower(city) in ('bogotá', 'bogota') then 'Bogotá'
                when lower(city) in ('medellín', 'medellin', 'medelin') then 'Medellín'
                when lower(city) in ('cali', 'cal') then 'Cali'
                when lower(city) = 'barranquilla' then 'Barranquilla'
                when lower(city) in ('arequipa', 'areqipa') then 'Arequipa'
                when lower(city) = 'trujillo' then 'Trujillo'
                when lower(city) in ('guadalajara', 'guadaljara') then 'Guadalajara'
                when lower(city) = 'monterrey' then 'Monterrey'
                when lower(city) = 'buenos aires' then 'Buenos Aires'
                when lower(city) in ('cordoba', 'coroba') then 'Córdoba'
                when lower(city) = 'lima' then 'Lima'
                when lower(city) = 'rosario' then 'Rosario'
                when lower(city) in ('santiago', 'santigo') then 'Santiago'
                when lower(city) = 'ciudad de mexico' then 'Ciudad de México'
                when lower(city) = 'valparaíso' then 'Valparaíso'
                when lower(city) = 'concepcion' then 'Concepción'
                else city
            end
        ))                                                         as city,

        -- Limpieza de operator
        lower(trim(
            case
                when lower(operator) in ('claro', 'clar', 'cla ') then 'claro'
                when lower(operator) in ('movistar', 'movstr','movistr','mov') then 'movistar'
                when lower(operator) in ('tigo', 'tgo','tig') then 'tigo'
                when lower(operator) in ('wom', 'won','w0m') then 'wom'
                else operator
            end
        ))                                                         as operator,

        lower(trim(plan_type))                                     as plan_type,

        case
            when registration_date between '2000-01-01' and current_date
            then registration_date
        end                                                         as registration_date,

        lower(trim(status))                                         as status,

        case
            when credit_score between 0 and 1000 then credit_score
        end                                                         as credit_score,

        ingestion_timestamp
    from deduplicated
)

-- 2️⃣ Filtra filas inválidas: edad nula o email nulo rompen tests
select *
from limpiado
where age is not null
  and email is not null
  and customer_id is not null
