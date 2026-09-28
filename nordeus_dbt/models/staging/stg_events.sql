{{config(materialized='view')}}

select
    event_id,
    timestamp,
    event_type,
    user_id,
    country,
    device_os,
    username,
    session_state,
    map_id,
    opponent_id,
    outcome,
    event_timestamp

from read_parquet('../data/silver/events_enriched/*.parquet')