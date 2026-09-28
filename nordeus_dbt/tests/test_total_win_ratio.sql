select *

from {{ ref('mart_player_stats') }}

where total_win_ratio < 0
   or total_win_ratio > 1