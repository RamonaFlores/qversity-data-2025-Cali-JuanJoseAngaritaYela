{{ config(materialized='table') }}

-- Bronze layer: Raw qversity content interactions from learning platform
select 
    'student_001' as user_id,
    'qversity_python_fundamentals' as qversity_id,
    'video' as content_type,
    'python_data_types' as content_id,
    'Python Data Types and Variables' as content_title,
    1350 as watch_time_seconds,
    true as completed,
    '2024-01-15 09:30:00'::timestamp as interaction_timestamp

union all

select 
    'student_001' as user_id,
    'qversity_python_fundamentals' as qversity_id,
    'document' as content_type,
    'python_syntax_guide' as content_id,
    'Python Syntax Reference Guide' as content_title,
    0 as watch_time_seconds,
    true as completed,
    '2024-01-15 09:55:00'::timestamp as interaction_timestamp

union all

select 
    'student_002' as user_id,
    'qversity_airflow_orchestration' as qversity_id,
    'video' as content_type,
    'airflow_dags_intro' as content_id,
    'Introduction to Airflow DAGs' as content_title,
    1680 as watch_time_seconds,
    true as completed,
    '2024-01-15 14:20:00'::timestamp as interaction_timestamp

union all

select 
    'student_002' as user_id,
    'qversity_airflow_orchestration' as qversity_id,
    'quiz' as content_type,
    'airflow_concepts_quiz' as content_id,
    'Airflow Core Concepts Quiz' as content_title,
    420 as watch_time_seconds,
    true as completed,
    '2024-01-15 14:50:00'::timestamp as interaction_timestamp

union all

select 
    'student_003' as user_id,
    'qversity_dbt_transformations' as qversity_id,
    'video' as content_type,
    'dbt_models_bronze_silver_gold' as content_id,
    'dbt Models: Bronze, Silver, Gold Architecture' as content_title,
    2100 as watch_time_seconds,
    true as completed,
    '2024-01-16 10:15:00'::timestamp as interaction_timestamp

union all

select 
    'student_003' as user_id,
    'qversity_dbt_transformations' as qversity_id,
    'document' as content_type,
    'dbt_jinja_templates' as content_id,
    'dbt Jinja Templates and Macros' as content_title,
    0 as watch_time_seconds,
    false as completed,
    '2024-01-16 11:00:00'::timestamp as interaction_timestamp

union all

select 
    'student_004' as user_id,
    'qversity_dwh_design' as qversity_id,
    'video' as content_type,
    'star_schema_modeling' as content_id,
    'Data Warehouse Star Schema Modeling' as content_title,
    1890 as watch_time_seconds,
    true as completed,
    '2024-01-17 13:30:00'::timestamp as interaction_timestamp

union all

select 
    'student_004' as user_id,
    'qversity_dwh_design' as qversity_id,
    'quiz' as content_type,
    'dimensional_modeling_quiz' as content_id,
    'Dimensional Modeling Concepts Quiz' as content_title,
    380 as watch_time_seconds,
    true as completed,
    '2024-01-17 14:15:00'::timestamp as interaction_timestamp 