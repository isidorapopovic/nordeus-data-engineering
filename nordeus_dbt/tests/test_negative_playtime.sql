select *

from {{ ref('mart_player_stats') }}

where total_playtime < 0