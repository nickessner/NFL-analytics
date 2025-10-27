{{ config(
    materialized='table',
    partition_by={
      "field": "season",
      "data_type": "int64",
      "range": {"start": 2019, "end": 2026, "interval": 1}
    },
    cluster_by=["posteam","play_type_simple"]
) }}


with raw as (
  select
    -- ids
    cast(season as int64) as season,
    game_id,

    -- teams
    posteam,
    home_team,
    away_team,

    -- opponent: if posteam is home, opp = away (and vice versa)
    case
      when posteam = home_team then away_team
      when posteam = away_team then home_team
      else null
    end as opponent,

    -- context
    play_type_simple,
    play_type,
    cast(yards_gained as float64) as yards_gained,
    cast(epa as float64) as epa,
    cast(success as float64) as success,

    -- situational controls you care about
    cast(shotgun as float64) as shotgun,        -- source supplies as 0/1 floats
    cast(no_huddle as float64) as no_huddle,
    cast(qb_dropback as float64) as qb_dropback,
    cast(xpass as float64) as xpass,
    offense_formation,

    -- keep common game state fields you mentioned (when present)
    cast(game_seconds_remaining as float64) as game_seconds_remaining,
    cast(score_differential as float64) as score_differential
  from {{ source('nfl_raw','pbp_2019_2023') }}
  where play_type_simple is not null
),

-- per-game play index (useful later)
indexed as (
  select
    *,
    row_number() over (
      partition by game_id
      order by game_seconds_remaining desc
    ) as play_idx
  from raw
)

select * from indexed

