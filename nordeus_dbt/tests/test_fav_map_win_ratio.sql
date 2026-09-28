select *

from {{ ref('mart_player_stats') }}

where fav_map_win_ratio < 0
   or fav_map_win_ratio > 1