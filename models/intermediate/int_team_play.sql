{{ config(materialized='view') }}

select
  season,
  game_id,
  play_idx,
  posteam,
  opponent,
  play_type_simple,
  play_type,
  epa,
  success,
  yards_gained,

  -- situational flags and priors
  shotgun,
  no_huddle,
  qb_dropback,
  xpass,
  offense_formation,

  game_seconds_remaining,
  score_differential
from {{ ref('stg_pbp') }}
