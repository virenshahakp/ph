{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort=['playback_session_id', 'loaded_at', 'dbt_processed_at', 'timestamp']
    , on_schema_change='append_new_columns'
  )
}}

{%- set platform_loaded_at =
  incremental_max_event_type_value('loaded_at') 
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
  from (
    {{ dbt_utils.union_relations(
        relations=[
            ref('derived_android_prod_watched_range_stage')
          , ref('derived_androidtv_prod_watched_range_stage')
          , ref('derived_fire_prod_watched_range_stage')
          , ref('derived_fire_tv_prod_watched_range_stage')
          , ref('derived_roku_prod_watched_range_stage')
          , ref('derived_ios_prod_watched_range_stage')
          , ref('derived_tvos_prod_watched_range_stage')
          , ref('derived_web_prod_watched_range_stage')
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
          , "platform"
          , "loaded_at"
        ]
    ) }}
  )
  {%- if target.name != 'prod' %}
    where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
  {%- endif %}

)

, playback_sessions_to_process as (

  select playback_session_id
  from all_platforms
  {%- if is_incremental() %}
    {%- if platform_loaded_at %}
      where
        -- ensure that any new platform is included
        all_platforms.platform not in (
          {%- for platform in platform_loaded_at['platform'] %}
            '{{ platform }}'
            {% if not loop.last %}, {% endif -%}
          {%- endfor -%}
        )

        {%- for index in range(platform_loaded_at['platform'] | length) -%}
          -- for each existing platform get the incremental timestamp specific to that platform
          or (
            all_platforms.platform = '{{ platform_loaded_at["platform"][index] }}'
            and all_platforms.loaded_at > '{{ platform_loaded_at["max_dbt_processed_at"][index] }}'::timestamp
          )
        {%- endfor %}
    {%- endif %}
  {%- endif %}

)

, add_data_server_attributes as (

  select
    all_platforms.event_id
    , all_platforms.user_id
    , all_platforms.playback_session_id
    , all_platforms.asset_id -- legacy column support
    , all_platforms.asset_id                                                  as requested_asset_id
    , playback_sessions.played_asset_id
    , all_platforms.received_at
    , all_platforms.timestamp_start
    , all_platforms."timestamp"
    , all_platforms.delay
    , all_platforms.hashed_session_id
    , all_platforms.context_ip
    , all_platforms.platform
    -- platforms_derived_watched_ranges don't contain bitrate 
    , null::bigint                                                            as bitrate
    , all_platforms.loaded_at
    , all_platforms.position_start * 1.0                                      as position_start
    , all_platforms.position_stop * 1.0                                       as position_stop
    , coalesce(guide_requested.channel_id, guide_played.channel_id)           as channel_id
    , coalesce(guide_requested.show_id, guide_played.show_id)                 as show_id
    , coalesce(guide_requested.episode_id, guide_played.episode_id)           as episode_id
    , coalesce(guide_requested.run_time, guide_played.run_time)               as run_time
    , coalesce(guide_requested.tms_series_id, guide_played.tms_series_id)     as tms_series_id
    , coalesce(guide_requested.philo_series_id, guide_played.philo_series_id) as philo_series_id
    , coalesce(guide_requested.asset_type, guide_played.asset_type)           as asset_type
  from all_platforms
  left join playback_sessions on (all_platforms.playback_session_id = playback_sessions.playback_session_id)
  left join guide as guide_requested on (all_platforms.asset_id = guide_requested.asset_id)
  left join guide as guide_played on (playback_sessions.played_asset_id = guide_played.asset_id)
  where all_platforms.playback_session_id in (
      -- records with new data
      select playback_session_id from playback_sessions_to_process
      union all
      -- recent records, already processed but with no matching dataserver record
      select playback_session_id from {{ this }}
      where played_asset_id is null
        and received_at > current_date - interval '10 days'
    )

)

select
  add_data_server_attributes.*
  , add_data_server_attributes.position_stop
  - add_data_server_attributes.position_start                                                                                                                                                                                                                                      as watched_seconds
  , {{ dbt_utils.generate_surrogate_key([
      'user_id'
      , 'playback_session_id'
      , 'requested_asset_id'
      , 'played_asset_id'
      , 'platform'
      ])
    }} as watched_minutes_id
  , sysdate                                                                                                                                                                                                                                                                        as dbt_processed_at
from add_data_server_attributes
where user_id is not null
  and asset_id is not null
  and playback_session_id is not null