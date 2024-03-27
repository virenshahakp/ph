{{ config(materialized='ephemeral') }}
with

broadcasts as (

  select * from {{ ref('guide_broadcasts_source') }}

)

-- There are duplicates in the source data which we need to clean up. For now,
-- de-duplicate here. There are also records that are almost identical except
-- on has an episode_id and the other does not.
, dedup_episode_id as (

  select
    broadcast_id
    , asset_id
    , asset_type
    , is_premiere
    , is_new
    , channel_id
    , show_id
    , starts_at
    , ends_at
    , has_audio_description
    , run_time
    , last_value(episode_id) over (
      partition by
        broadcast_id
        , asset_id
      order by episode_id rows between unbounded preceding and unbounded following
    ) as episode_id
  from broadcasts
)

-- there is also one duplicate instance with different run times; the difference
-- is ends_at
, dedup_run_time as (
  select
    broadcast_id
    , asset_id
    , asset_type
    , is_premiere
    , is_new
    , channel_id
    , show_id
    , episode_id
    , starts_at
    , has_audio_description
    , min(ends_at)  as ends_at
    , min(run_time) as run_time
  from dedup_episode_id
  {{ dbt_utils.group_by(n=10) }}
)

select
  broadcast_id
  , asset_id
  , asset_type
  , is_premiere
  , is_new
  , channel_id
  , show_id
  , episode_id
  , starts_at
  , ends_at
  , run_time
  , has_audio_description
  , first_value(
    case when is_premiere is true then starts_at end ignore nulls
  ) over (
    partition by show_id, episode_id
    order by starts_at asc
    rows between unbounded preceding and unbounded following
  ) as premiered_at
  , first_value(
    case when is_new is true then starts_at end ignore nulls
  ) over (
    partition by show_id, episode_id
    order by starts_at asc
    rows between unbounded preceding and unbounded following
  ) as new_at
from dedup_run_time
{{ dbt_utils.group_by(n=12) }}

