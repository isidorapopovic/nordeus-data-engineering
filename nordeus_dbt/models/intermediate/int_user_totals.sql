{{ config(materialized='table') }}

select
    user_id,

    sum(duration) as total_playtime,

    sum(is_win) as total_wins,

    count(*) as total_matches,

    count(distinct session_number) as total_sessions,

    sum(is_win) * 1.0 / count(*) as total_win_ratio,

    count(*) * 1.0
        / nullif(count(distinct session_number), 0)
        as avg_matches_per_session

from {{ ref('int_matches') }}

group by user_id