{{ config(materialized='table') }}

-- Silver layer: Cleaned and enriched qversity content engagement metrics
select 
    user_id,
    qversity_id,
    case 
        when qversity_id = 'qversity_python_fundamentals' then 'Python Fundamentals'
        when qversity_id = 'qversity_airflow_orchestration' then 'Airflow Orchestration'
        when qversity_id = 'qversity_dbt_transformations' then 'dbt Transformations'
        when qversity_id = 'qversity_dwh_design' then 'Data Warehouse Design'
        else qversity_id
    end as qversity_name,
    content_type,
    content_id,
    content_title,
    case 
        when content_type = 'video' then watch_time_seconds / 60.0
        else 0 
    end as watch_time_minutes,
    case 
        when content_type = 'video' and watch_time_seconds >= 900 then true  -- 15 min minimum for videos
        when content_type in ('document', 'quiz') and completed then true
        else false 
    end as engagement_threshold_met,
    completed,
    interaction_timestamp,
    extract(hour from interaction_timestamp) as interaction_hour,
    extract(dow from interaction_timestamp) as interaction_day_of_week,
    current_timestamp as processed_at
from {{ ref('bronze_qversity_interactions') }} 