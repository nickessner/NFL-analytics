{{ config(
    materialized='table',
    partition_by={"field": "season", "data_type": "int64"},
    cluster_by=["posteam","opponent"]
) }}

with base as (select * from {{ ref('int_team_play') }})

select
  season,
  game_id,
  posteam,
  opponent,
  count(*) as plays,
  sum(yards_gained) as yards_total,
  avg(yards_gained) as ypp,
  sum(epa) as epa_total,
  avg(epa) as epa_per_play,
  avg(success) as success_rate,
  sum(case when play_type_simple='pass' then 1 else 0 end) as pass_plays,
  sum(case when play_type_simple='rush' then 1 else 0 end) as rush_plays
from base
group by 1,2,3,4
