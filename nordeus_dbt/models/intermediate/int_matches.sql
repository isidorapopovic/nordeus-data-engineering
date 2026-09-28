{{ config(
    materialized='incremental',
    unique_key=['user_id', 'map_id', 'opponent_id', 'match_number']
) }}

with starts as (

    select *
    from {{ ref('int_match_starts') }}

),

finishes as (

    select *
    from {{ ref('int_match_finishes') }}

),

matches as (

    select
        s.user_id,
        s.map_id,
        s.opponent_id,
        s.session_number,
        s.match_number,
        s.start_timestamp,
        f.finish_timestamp,
        f.finish_timestamp - s.start_timestamp as duration,
        f.outcome,

        case
            when f.outcome = 1 then 1
            else 0
        end as is_win

    from starts s

    inner join finishes f
        on s.user_id = f.user_id
        and s.map_id = f.map_id
        and s.opponent_id = f.opponent_id
        and s.match_number = f.match_number

    where f.finish_timestamp >= s.start_timestamp

)

select *
from matches

{% if is_incremental() %}

where finish_timestamp > (
    select coalesce(max(finish_timestamp), 0)
    from {{ this }}
)

{% endif %}