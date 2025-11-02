{{ config(
    materialized='table',
    partition_by={
      "field": "season",
      "data_type": "int64",
      "range": {"start": 2019, "end": 2023, "interval": 1}
    },
    cluster_by=["posteam","opponent","play_type_simple","offense_formation"]
) }}

with base as (
  select * from {{ ref('int_team_play') }}
)

select
  season,
  posteam,
  opponent,
  play_type_simple,
  offense_formation,

  -- keep the flags as dimensions if you want to slice by them;
  -- or switch them to metrics if you'd rather average their rates
  cast(shotgun as int64) as shotgun_flag,
  cast(no_huddle as int64) as no_huddle_flag,
  cast(qb_dropback as int64) as qb_dropback_flag,

  -- metrics
  count(*) as plays,
  sum(yards_gained) as yards_total,
  avg(yards_gained) as yards_per_play,
  sum(epa) as epa_total,
  avg(epa) as epa_per_play,
  avg(success) as success_rate,
  avg(xpass) as xpass_avg,

  -- simple pass/rush split off play_type_simple
  sum(case when play_type_simple = 'pass' then 1 else 0 end) as pass_plays,
  sum(case when play_type_simple = 'rush' then 1 else 0 end) as rush_plays
from base
group by 1,2,3,4,5,6,7,8
