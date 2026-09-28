{{ config(materialized='view') }}

select
    user_id,
    map_id,
    opponent_id,
    outcome,
    timestamp as finish_timestamp,

    row_number() over (
        partition by user_id, map_id, opponent_id
        order by timestamp
    ) as match_number

from {{ ref('int_events_sessions') }}

where event_type = 'match_finish'