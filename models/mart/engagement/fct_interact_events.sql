{{
  config(
    materialized='incremental'
    , unique_key='id'
    , dist='even'
    , sort=['event_timestamp']
  )
}}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('received_at') 
%}

with

-- note that chromecast is intentionally excluded since it does not generate interacts
all_platforms as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_interact_stage')
        , ref('androidtv_prod_interact_stage')
        , ref('fire_prod_interact_stage')
        , ref('fire_tv_prod_interact_stage')
        , ref('ios_prod_interact_stage')
        , ref('roku_prod_interact_stage')
        , ref('tvos_prod_interact_stage')
        , ref('web_prod_interact_stage')
      ]
      , include=interact_columns()
    )
  }}

)

, add_platform as (

  select
    all_platforms.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from all_platforms

)

-- noqa: disable=all
-- issue with jinja formatting, passing in dev but not in gitlab
select add_platform.*
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
          and received_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
        )
      {%- endfor -%}
  {%- endif %}
{%- endif %}
