{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , dist='playback_session_id'
    , sort='received_at'
    , on_schema_change='append_new_columns'
  )
}}
-- consider sort change to something like
-- , sort=['dbt_processed_at', 'event_timestamp', 'playback_session_id', 'event_id']

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('dbt_processed_at') 
%}

with

user_playback_sessions as (

  select * from {{ ref('dataserver_prod_user_playback_session_map') }}

)


, all_platforms as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_rebuffering_end_stage')
        , ref('androidtv_prod_rebuffering_end_stage')
        , ref('chromecast_prod_rebuffering_end_stage')
        , ref('fire_prod_rebuffering_end_stage')
        , ref('fire_tv_prod_rebuffering_end_stage')
        , ref('ios_prod_rebuffering_end_stage')
        , ref('roku_prod_rebuffering_end_stage')
        , ref('samsung_prod_rebuffering_end_stage')
        , ref('tvos_prod_rebuffering_end_stage')
        , ref('viziotv_prod_rebuffering_end_stage')
        , ref('web_prod_rebuffering_end_stage')
      ]
      , include=qoe_columns(additional_columns=[
        'duration'
        , 'dbt_processed_at'
      ])
    )
  }}

)

, add_platform as (

  select
    id
    , user_id
    , hashed_session_id
    , event_timestamp
    , received_at
    , app_version
    , analytics_version
    , os_version
    , client_ip
    , device_name
    , device_manufacturer
    , device_model
    , event
    , playback_session_id
    , asset_id
    , adapted_bitrate
    , user_selected_bitrate
    , estimated_bandwidth
    , is_wifi
    , is_cellular
    , position
    , screen_height
    , screen_width
    , loaded_at
    , dbt_processed_at
    , duration
    , {{ qoe_event_id() }}
    , {{ get_platform_from_union_relations(_dbt_source_relation) }}
    as platform
  from all_platforms
  where playback_session_id is not null

)


-- identify incrementally new events and add data server attributes
, new_events as (
  -- noqa: disable=all
  select
    add_platform.id
    , add_platform.hashed_session_id
    , add_platform.event_timestamp
    , add_platform.received_at
    , add_platform.app_version
    , add_platform.analytics_version
    , add_platform.os_version
    , add_platform.client_ip
    , add_platform.device_name
    , add_platform.device_manufacturer
    , add_platform.device_model
    , add_platform.event
    , add_platform.playback_session_id
    , add_platform.asset_id
    , add_platform.adapted_bitrate
    , add_platform.user_selected_bitrate
    , add_platform.estimated_bandwidth
    , add_platform.is_wifi
    , add_platform.is_cellular
    , add_platform.position
    , add_platform.screen_height
    , add_platform.screen_width
    , add_platform.loaded_at
    , add_platform.duration
    , add_platform.event_id
    , add_platform.platform
    , user_playback_sessions.played_asset_id                         as played_asset_id
    , coalesce(add_platform.user_id, user_playback_sessions.user_id) as user_id
    , sysdate                                                        as dbt_processed_at
  from add_platform
  left join user_playback_sessions on (add_platform.playback_session_id = user_playback_sessions.playback_session_id)
  {%- if is_incremental() %}
    {%- if platform_dbt_processed_at %}
      where 
        -- ensure that any new platform is included
        add_platform.platform not in (
          {%- for platform in platform_dbt_processed_at['platform'] %}
            '{{ platform }}'
            {% if not loop.last %}, {% endif -%}
          {%- endfor -%})
      
        {%- for index in range(platform_dbt_processed_at['platform'] | length) %}
          -- for each existing platform get the incremental timestamp specific to that platform
          or (
            add_platform.platform = '{{ platform_dbt_processed_at["platform"][index] }}'
            and add_platform.dbt_processed_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
          )
        {%- endfor %}
    {%- endif %}
  {%- endif %}
  -- noqa: enable=all

)

{% if is_incremental() %}
  -- any existing events that did not previously have a matching playback session should be
  -- improved to have user_id and asset data appended
  , existing_events as (

    select
      rebuffering_ends.id
      , rebuffering_ends.hashed_session_id
      , rebuffering_ends.event_timestamp
      , rebuffering_ends.received_at
      , rebuffering_ends.app_version
      , rebuffering_ends.analytics_version
      , rebuffering_ends.os_version
      , rebuffering_ends.client_ip
      , rebuffering_ends.device_name
      , rebuffering_ends.device_manufacturer
      , rebuffering_ends.device_model
      , rebuffering_ends.event
      , rebuffering_ends.playback_session_id
      , rebuffering_ends.asset_id
      , rebuffering_ends.adapted_bitrate
      , rebuffering_ends.user_selected_bitrate
      , rebuffering_ends.estimated_bandwidth
      , rebuffering_ends.is_wifi
      , rebuffering_ends.is_cellular
      , rebuffering_ends.position
      , rebuffering_ends.screen_height
      , rebuffering_ends.screen_width
      , rebuffering_ends.loaded_at
      , rebuffering_ends.duration
      , rebuffering_ends.event_id
      , rebuffering_ends.platform
      , coalesce(rebuffering_ends.played_asset_id, user_playback_sessions.played_asset_id) as played_asset_id
      , coalesce(rebuffering_ends.user_id, user_playback_sessions.user_id)                 as user_id
      , sysdate                                                                            as dbt_processed_at
    from {{ this }} as rebuffering_ends --noqa: L031
    join user_playback_sessions on (rebuffering_ends.playback_session_id = user_playback_sessions.playback_session_id)
    where
      (
        rebuffering_ends.user_id is null
        or rebuffering_ends.played_asset_id is null
      )
      -- only attempt to add playback session data for 4 weeks after initially processing the event
      and rebuffering_ends.dbt_processed_at > current_date - interval '28 days'

  )
{% endif %}

select * from new_events
{% if is_incremental() %}
  union all
  select * from existing_events
{% endif %}