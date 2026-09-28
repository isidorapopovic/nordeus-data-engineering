{{ config(materialized='table') }}

with registrations as (

    select
        user_id,
        max(username) as username,
        max(country) as country,
        min(event_timestamp)::date as registration_date

    from {{ ref('stg_events') }}

    where event_type = 'registration'

    group by user_id

),

ranked_maps as (

    select
        ums.user_id,
        ums.map_id,
        m.map_name,
        ums.map_matches,
        ums.map_win_ratio,

        row_number() over (
            partition by ums.user_id
            order by
                ums.map_win_ratio desc,
                ums.map_matches desc,
                m.map_name asc
        ) as map_rank

    from {{ ref('int_user_map_stats') }} ums

    left join {{ ref('stg_maps') }} m
        on ums.map_id = m.map_id

),

favorite_maps as (

    select
        user_id,
        map_name as fav_map,
        map_win_ratio as fav_map_win_ratio

    from ranked_maps

    where map_rank = 1

),

totals as (

    select * from {{ ref('int_user_totals') }}

)

select
    r.user_id,
    r.username,
    r.country,
    f.fav_map,
    round(f.fav_map_win_ratio, 3) as fav_map_win_ratio,
    t.total_playtime,
    round(t.total_win_ratio, 3) as total_win_ratio,
    round(t.avg_matches_per_session, 2) as avg_matches_per_session,
    r.registration_date,

    case
        when r.country is null then 'missing_country'
        else 'valid'
    end as data_quality_status

from registrations r
left join totals t on r.user_id = t.user_id
left join favorite_maps f on r.user_id = f.user_id

order by t.total_playtime desc nulls last