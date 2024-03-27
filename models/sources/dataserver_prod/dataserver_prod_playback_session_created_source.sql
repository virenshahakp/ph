with

session_created as (

  select * from {{ source('dataserver_prod', 'playback_session_created') }}

)

, renamed as (

  select
    user_id                                                                                 as user_id
    , received_at                                                                           as received_at
    , uuid_ts                                                                               as loaded_at
    , pid                                                                                   as playback_session_id
    , sutured_pid                                                                           as sutured_pid
    , "timestamp"                                                                           as session_created_at
    , is_new_session
    , environment_analytics_version
    , manifest_environment
    , as_number
    , as_name
    , geohash
    , dma
    , is_targeting                                                                          as is_sender
    , player_id
    , shared_playback_session_id
    , ad_breaks
    , use_audio_description
    , {{- normalize_id("requested_asset_id") }}
    as requested_asset_id
    , coalesce( 
      {{- normalize_id("played_asset_id") }}, {{- normalize_id("asset_id") }}
    )                                                                                       as played_asset_id
    , lower(synthetic_channel_id)                                                           as synthetic_channel_id
    , upper(asset_type)                                                                     as played_asset_type
    , {{- normalize_id("tile_group_id") }}
    as tile_group_id
    , coalesce(content_cdnurl, content_cdnhost, 'content-us-east-2-fastly-b.www.philo.com') as content_cdn_host
  from session_created
  where pid is not null

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
