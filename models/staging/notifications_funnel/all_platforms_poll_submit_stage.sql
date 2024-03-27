{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , dist='user_id'
    , sort=['user_id', 'received_at']
  )
}}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('dbt_processed_at') 
%}

with

poll_submit as (

  {{ 
    dbt_utils.union_relations(
      relations=[
        ref('android_prod_poll_submit_stage')
        , ref('androidtv_prod_poll_submit_stage')
        , ref('fire_prod_poll_submit_stage')
        , ref('fire_tv_prod_poll_submit_stage')
        , ref('ios_prod_poll_submit_stage')
        , ref('tvos_prod_poll_submit_stage')
        , ref('roku_prod_poll_submit_stage')
        , ref('web_prod_poll_submit_stage')
      ]
    )
  }}

)

, add_platform as (

  select
    poll_submit.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from poll_submit

)

select
  event_id
  , platform
  , user_id
  , poll_name
  , question
  , answer
  , environment_analytics_version
  , received_at
  , loaded_at
  , sysdate as dbt_processed_at
from add_platform
{%- if is_incremental() %}
{% if platform_dbt_processed_at %}
where 
  -- ensure that any new platform is included
  platform not in (
    {%- for platform in platform_dbt_processed_at['platform'] %} '{{ platform }}'
  {% if not loop.last %},{% endif -%}
    {%- endfor -%})
  -- for each existing platform get the incremental timestamp specific to that platform
  {%- for index in range(platform_dbt_processed_at['platform'] | length) %}
or (
  platform = '{{ platform_dbt_processed_at["platform"][index] }}'
  and add_platform.dbt_processed_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
) 
{% endfor %}
{% endif %}
{%- endif %}
