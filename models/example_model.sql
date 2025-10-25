-- Example model selecting from your BigQuery table
select
  game_id,
  play_id,
  posteam,
  defteam,
  down,
  yards_gained
from `daring-emitter-469314-f5.nfl.pbp_2019_2023`
limit 1000
