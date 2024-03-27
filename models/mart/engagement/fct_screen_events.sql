{{
  config(
    materialized='incremental'
    , unique_key=['event_id']
    , dist='user_id'
    , sort=['event_timestamp']
  )
}}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('loaded_at') 
%}

with

all_platforms as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_screens_stage')
        , ref('androidtv_prod_screens_stage')
        , ref('chromecast_prod_pages_stage')
        , ref('fire_prod_screens_stage')
        , ref('fire_tv_prod_screens_stage')
        , ref('ios_prod_screens_stage')
        , ref('roku_prod_screens_stage')
        , ref('samsung_prod_pages_stage')
        , ref('tvos_prod_screens_stage')
        , ref('viziotv_prod_pages_stage')
        , ref('web_prod_pages_stage')
      ]
      , include=[
        "event_id"
        , "anonymous_id"
        , "user_id"
        , "event_timestamp"
        , "screen_name"
        , "environment_analytics_version"
        , "app_version"
        , "visited_at"
        , "loaded_at"
      ]
    )
  }}

)

, add_platform as (

  select
    all_platforms.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from all_platforms
  {%- if target.name != 'prod' %}
    where event_timestamp >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
      or
      visited_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }} 
  {%- endif %}

)

select
  event_id
  , anonymous_id
  , user_id
  , screen_name
  , environment_analytics_version
  , platform
  , loaded_at
  , app_version
  , coalesce(event_timestamp, visited_at) as event_timestamp
  , sysdate                               as dbt_processed_at
from add_platform
{%- if is_incremental() %}
  {%- if platform_dbt_processed_at %}
    where
      -- ensure that any new platform is included
      platform not in (
        {% for platform in platform_dbt_processed_at['platform'] -%}
          '{{ platform }}'
          {%- if not loop.last %},{%- endif -%}
        {%- endfor %}
      )
     
      {%- for index in range(platform_dbt_processed_at['platform'] | length) -%}
        -- for each existing platform get the incremental timestamp specific to that platform
        or (
          platform = '{{ platform_dbt_processed_at["platform"][index] }}'
          and loaded_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
        )
      {%- endfor -%}
  {%- endif %}
{%- endif %}
