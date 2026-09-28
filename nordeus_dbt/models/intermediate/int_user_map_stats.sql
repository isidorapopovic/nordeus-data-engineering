{{ config(materialized='table') }}

select
    user_id,
    map_id,

    count(*) as map_matches,

    sum(is_win) as map_wins,

    sum(is_win) * 1.0 / count(*) as map_win_ratio

from {{ ref('int_matches') }}

group by
    user_id,
    map_id