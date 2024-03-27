with

broadcasts as (

  select * from {{ ref('guide_broadcasts_stage') }}

)

, channels as (

  select * from {{ ref('guide_channels_stage') }}

)

, shows as (

  select * from {{ ref('guide_shows_stage') }}

)

, episodes as (

  select * from {{ ref('guide_episodes_stage') }}

)

select
  broadcasts.asset_id                                             as asset_id
  , broadcasts.asset_type                                         as asset_type
  , broadcasts.is_premiere                                        as is_premiere
  , broadcasts.is_new                                             as is_new
  , broadcasts.premiered_at                                       as premiered_at
  , broadcasts.new_at                                             as new_at
  , broadcasts.run_time                                           as run_time
  , broadcasts.has_audio_description                              as has_audio_description
  , channels.channel_id                                           as channel_id
  , channels.callsign                                             as channel_callsign
  , channels.channel_name                                         as channel_name
  , channels.has_public_view                                      as has_public_view
  , channels.is_premium                                           as is_premium
  , channels.is_free                                              as is_free
  , shows.show_title                                              as show_title
  , shows.show_id                                                 as show_id
  , shows.content_type                                            as content_type
  , shows.root_show_id                                            as root_show_id
  , shows.run_time                                                as show_run_time
  , shows.tms_series_id                                           as tms_series_id
  , episodes.episode_title                                        as episode_title
  , episodes.episode_id                                           as episode_id
  , episodes.episode_num                                          as episode_num
  , episodes.season_num                                           as season_num
  , coalesce(episodes.original_air_date, shows.original_air_date) as original_air_date
from broadcasts
left join channels on (broadcasts.channel_id = channels.channel_id)
left join shows on (broadcasts.show_id = shows.show_id)
left join episodes on (broadcasts.episode_id = episodes.episode_id)
