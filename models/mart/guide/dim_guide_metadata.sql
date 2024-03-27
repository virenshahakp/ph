{{ config(materialized='table', sort=['asset_id', 'channel_episode_id', 'show_episode_id'], dist='ALL') }}
-- Use ALL distribution to place this table on all Redshift nodes so that joins
-- do not need to traverse the network.
with

all_guides as (

  {{ dbt_utils.union_relations(
      relations=[
          ref('guide_broadcasts_denormalized')
        , ref('guide_channels_denormalized')
        , ref('guide_recordings_denormalized')
        , ref('guide_vod_assets_denormalized')
      ],
      include=[
          "asset_id"
        , "asset_type"
        , "channel_id"
        , "channel_callsign"
        , "channel_name"
        , "tms_series_id"
        , "show_title"
        , "show_id"
        , "episode_title"
        , "episode_id"
        , "episode_num"
        , "season_num"
        , "original_air_date"
        , "license_start"
        , "license_end"
        , "new_at"
        , "premiered_at"
        , "run_time"
        , "content_type"
        , "is_premium"
        , "is_new"
        , "is_premiere"
        , "is_free"
        , "has_public_view"
        , "has_audio_description"
        , "root_show_id"
        , "show_run_time"
        , "tms_series_id"
      ]
  ) }}

)

, generate_philo_ids as (

  -- create show+episode and channel+show+episode ids for better content identification
  -- also adds in a Philo series ID to handle when content may change names but should remain
  -- as part of the same series
  select
    *
    , case
      when show_id is not null
        then {{ dbt_utils.generate_surrogate_key(['show_id', 'episode_id']) }}
    end                                                                                                                                                        as show_episode_id
    , {{ dbt_utils.generate_surrogate_key(['channel_id', 'show_id', 'episode_id']) }} as channel_episode_id
    , case
      when tms_series_id is not null and tms_series_id != ''
        then 'SERIES:' || tms_series_id
      when show_id is not null
        then 'SHOW:' || show_id
      else 'CHANNEL:' || channel_id
    end                                                                                                                                                        as philo_series_id
    , case
      when is_premiere = true
        then 'premiere'
      when is_new = true
        then 'new'
      else 'rebroadcast'
    end                                                                                                                                                        as show_status
  from all_guides

)

/*
determine the canonical name for shows that have multiple titles
we use the first show_id (lowest int) for each series_id
*/
select
  generate_philo_ids.*
  , first_value(show_title ignore nulls) over (
    partition by philo_series_id
    order by show_id, asset_id
    rows between unbounded preceding and unbounded following
  ) as series_title
from generate_philo_ids
