{{ config(materialized='view') }}

with events as (

    select * from {{ ref('stg_events') }}

),

session_flags as (

    select
        *,
        case
            when event_type = 'session_ping'
             and session_state = 'started'
            then 1
            else 0
        end as new_session

    from events

),

sessions as (

    select
        *,
        sum(new_session) over (
            partition by user_id
            order by timestamp, event_id
            rows between unbounded preceding and current row
        ) as session_number

    from session_flags

)

select * from sessions