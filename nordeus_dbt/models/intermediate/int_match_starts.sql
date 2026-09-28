{{ config(materialized='view') }}

select
    user_id,
    map_id,
    opponent_id,
    session_number,
    timestamp as start_timestamp,

    row_number() over (
        partition by user_id, map_id, opponent_id
        order by timestamp
    ) as match_number

from {{ ref('int_events_sessions') }}

where event_type = 'match_start'