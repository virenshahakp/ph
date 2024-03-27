{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort=['playback_session_id', 'dbt_processed_at', 'timestamp']
    , on_schema_change='append_new_columns'
  )
}}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('dbt_processed_at') 
%}

with

playback_sessions as (

  select * from {{ ref('dataserver_prod_playback_session_created_stage') }}

)

, guide as (

  select * from {{ ref('dim_guide_metadata') }}

)

, all_platforms as (

  select *
  from
  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_watched_ranges_stage')
        , ref('androidtv_prod_watched_ranges_stage')
        , ref('chromecast_prod_watched_ranges_stage')
        , ref('fire_prod_watched_ranges_stage')
        , ref('fire_tv_prod_watched_ranges_stage')
        , ref('ios_prod_watched_ranges_stage')
        , ref('roku_prod_watched_ranges_stage')
        , ref('samsung_prod_watched_ranges_stage')
        , ref('tvos_prod_watched_ranges_stage')
        , ref('viziotv_prod_watched_ranges_stage')
        , ref('web_prod_watched_ranges_stage')
      ],
      include=[
        "event_id"
        , "user_id"
        , "playback_session_id"
        , "asset_id"
        , "received_at"
        , "timestamp_start"
        , "timestamp"
        , "delay"
        , "position_start"
        , "position_stop"
        , "hashed_session_id"
        , "context_ip"
        , "action"
        , "bitrate"
        , "watched_seconds"
        , "loaded_at"
        , "dbt_processed_at"
      ]
  ) }}

)

, add_platform as (

  select
    all_platforms.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from all_platforms
  {%- if target.name != 'prod' %}
    where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
  {%- endif %}

)

, playback_sessions_to_process as (

  select playback_session_id
  from add_platform
  {%- if is_incremental() %}
    {%- if platform_dbt_processed_at %}
      where
        -- ensure that any new platform is included
        add_platform.platform not in (
          {%- for platform in platform_dbt_processed_at['platform'] %}
            '{{ platform }}'
            {% if not loop.last %}, {% endif -%}
          {%- endfor -%}
        )

        {%- for index in range(platform_dbt_processed_at['platform'] | length) -%}
          -- for each existing platform get the incremental timestamp specific to that platform
          or (
            add_platform.platform = '{{ platform_dbt_processed_at["platform"][index] }}'
            and add_platform.dbt_processed_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
          )
        {%- endfor %}
    {%- endif %}
  {%- endif %}

)

, add_data_server_attributes as (

  select
    add_platform.event_id
    , add_platform.playback_session_id
    , add_platform.received_at
    , add_platform.timestamp_start
    , add_platform."timestamp"
    , add_platform.delay
    , add_platform.position_start
    , add_platform.position_stop
    , add_platform.hashed_session_id
    , add_platform.context_ip
    , add_platform.platform
    , add_platform.action
    , add_platform.bitrate
    , add_platform.watched_seconds
    , playback_sessions.played_asset_id
    -- a set of values that are sent from both client and server can be taken preferentially from the client
    , coalesce(add_platform.user_id, playback_sessions.user_id)          as user_id
    , coalesce(add_platform.asset_id, playback_sessions.played_asset_id) as asset_id -- legacy column support
    -- requested asset id from the client may encode additional information beyond the guide asset identifier
    , coalesce(add_platform.asset_id, playback_sessions.played_asset_id) as requested_asset_id
  from add_platform
  left join playback_sessions on (add_platform.playback_session_id = playback_sessions.playback_session_id)
  where add_platform.playback_session_id in (
      -- records with new data
      select playback_session_id from playback_sessions_to_process
      union all
      -- recent records, already processed but with no matching dataserver record
      select playback_session_id from {{ this }}
      where played_asset_id is null
        and received_at > current_date - interval '10 days'
    )

)

, add_guide_data as (

  select
    add_data_server_attributes.event_id
    , add_data_server_attributes.playback_session_id
    , add_data_server_attributes.received_at
    , add_data_server_attributes.timestamp_start
    , add_data_server_attributes."timestamp"
    , add_data_server_attributes.delay
    , add_data_server_attributes.position_start
    , add_data_server_attributes.position_stop
    , add_data_server_attributes.hashed_session_id
    , add_data_server_attributes.context_ip
    , add_data_server_attributes.platform
    , add_data_server_attributes.action
    , add_data_server_attributes.bitrate
    , add_data_server_attributes.watched_seconds
    , add_data_server_attributes.played_asset_id
    , add_data_server_attributes.user_id
    , add_data_server_attributes.asset_id -- legacy column support
    , add_data_server_attributes.requested_asset_id
    -- gather guide data prefer using the requested asset details, but fall back to the played asset
    , coalesce(guide_requested.channel_id, guide_played.channel_id)           as channel_id
    , coalesce(guide_requested.show_id, guide_played.show_id)                 as show_id
    , coalesce(guide_requested.episode_id, guide_played.episode_id)           as episode_id
    , coalesce(guide_requested.run_time, guide_played.run_time)               as run_time
    , coalesce(guide_requested.tms_series_id, guide_played.tms_series_id)     as tms_series_id
    , coalesce(guide_requested.philo_series_id, guide_played.philo_series_id) as philo_series_id
    , coalesce(guide_requested.asset_type, guide_played.asset_type)           as asset_type
  from add_data_server_attributes
  left join guide as guide_requested on (add_data_server_attributes.asset_id = guide_requested.asset_id)
  left join guide as guide_played on (add_data_server_attributes.played_asset_id = guide_played.asset_id)

)

select
  add_guide_data.*
  , {{ dbt_utils.generate_surrogate_key([
      'user_id'
      , 'playback_session_id'
      , 'requested_asset_id'
      , 'played_asset_id'
      , 'platform'
    ])
  }} as watched_minutes_id
  , sysdate                                                                                                                                                                                                                                                                        as dbt_processed_at
from add_guide_data
where user_id is not null
  and asset_id is not null
  and playback_session_id is not null
  and watched_seconds is not null