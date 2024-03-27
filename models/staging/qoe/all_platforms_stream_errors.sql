{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , dist='playback_session_id'
    , sort='received_at'
  )
}}
-- consider sort change to something like
-- , sort=['dbt_processed_at', 'event_timestamp', 'playback_session_id', 'event_id']

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('dbt_processed_at') 
%}

with

-- this allows backfilling when user_id or played_asset_id is missing in the error event
user_playback_sessions as (

  select * from {{ ref('dataserver_prod_user_playback_session_map') }}

)

-- combine data from all of our platforms
, all_platforms as (

  {{ dbt_utils.union_relations(
    relations=[
      ref('android_prod_stream_error_stage')
      , ref('androidtv_prod_stream_error_stage')
      , ref('chromecast_prod_stream_error_stage')
      , ref('fire_prod_stream_error_stage')
      , ref('fire_tv_prod_stream_error_stage')
      , ref('ios_prod_stream_error_stage')
      , ref('roku_prod_stream_error_stage')
      , ref('samsung_prod_stream_error_stage')
      , ref('tvos_prod_stream_error_stage')
      , ref('viziotv_prod_stream_error_stage')
      , ref('web_prod_stream_error_stage')
    ]
    , include=qoe_columns(additional_columns=[
      'error_code'
      , 'error_description'
      , 'error_philo_code'
      , 'error_detailed_name'
      , 'error_http_status_code'
      , 'dbt_processed_at'
    ])
  ) }}

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
    , error_code
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , dbt_processed_at
    , coalesce(error_description, error_code::varchar(256))
    as error_description
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
    , add_platform.error_code
    , add_platform.error_description
    , add_platform.error_philo_code
    , add_platform.error_detailed_name
    , add_platform.error_http_status_code
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
  -- noqa: ensable=all

)

{% if is_incremental() %}
  -- any existing events that did not previously have a matching playback session should be
  -- improved to have user_id and asset data appended
  , existing_events as (

    select
      stream_errors.id
      , stream_errors.hashed_session_id
      , stream_errors.event_timestamp
      , stream_errors.received_at
      , stream_errors.app_version
      , stream_errors.analytics_version
      , stream_errors.os_version
      , stream_errors.client_ip
      , stream_errors.device_name
      , stream_errors.device_manufacturer
      , stream_errors.device_model
      , stream_errors.event
      , stream_errors.playback_session_id
      , stream_errors.asset_id
      , stream_errors.adapted_bitrate
      , stream_errors.user_selected_bitrate
      , stream_errors.estimated_bandwidth
      , stream_errors.is_wifi
      , stream_errors.is_cellular
      , stream_errors.position
      , stream_errors.screen_height
      , stream_errors.screen_width
      , stream_errors.loaded_at
      , stream_errors.error_code
      , stream_errors.error_description
      , stream_errors.error_philo_code
      , stream_errors.error_detailed_name
      , stream_errors.error_http_status_code
      , stream_errors.event_id
      , stream_errors.platform
      , coalesce(stream_errors.played_asset_id, user_playback_sessions.played_asset_id) as played_asset_id
      , coalesce(stream_errors.user_id, user_playback_sessions.user_id)                 as user_id
      , sysdate                                                                         as dbt_processed_at
    from {{ this }} as stream_errors --noqa: L031
    join user_playback_sessions on (stream_errors.playback_session_id = user_playback_sessions.playback_session_id)
    where
      (
        stream_errors.user_id is null
        or stream_errors.played_asset_id is null
      )
      -- only attempt to add playback session data for 4 weeks after initially processing the event
      and stream_errors.dbt_processed_at > current_date - interval '28 days'

  )
{% endif %}

select * from new_events
{% if is_incremental() %}
  union all
  select * from existing_events
{% endif %}