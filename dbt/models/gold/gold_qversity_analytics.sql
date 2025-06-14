{{ config(materialized='table') }}

-- Gold layer: Qversity learning analytics and performance metrics
select 
    qversity_name,
    qversity_id,
    content_type,
    count(distinct user_id) as unique_students,
    count(*) as total_interactions,
    round(avg(watch_time_minutes), 2) as avg_watch_time_minutes,
    round(
        count(case when completed then 1 end)::numeric / count(*)::numeric * 100, 
        2
    ) as completion_rate_percentage,
    round(
        count(case when engagement_threshold_met then 1 end)::numeric / count(*)::numeric * 100, 
        2
    ) as deep_engagement_rate_percentage,
    count(case when interaction_hour between 9 and 17 then 1 end) as business_hours_interactions,
    count(case when interaction_day_of_week in (1,2,3,4,5) then 1 end) as weekday_interactions,
    min(interaction_timestamp) as first_interaction,
    max(interaction_timestamp) as latest_interaction,
    current_timestamp as report_generated_at
from {{ ref('silver_qversity_engagement') }}
group by qversity_name, qversity_id, content_type
order by unique_students desc, completion_rate_percentage desc 