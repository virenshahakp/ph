{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , sort='dbt_processed_at'
    , dist='event_id' 
  )
}}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('dbt_processed_at') 
%}

with

dialog_displayed as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('ios_prod_dialog_displayed_stage')
      ]
    )
  }}

)

, add_platform as (

  select
    dialog_displayed.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from dialog_displayed

)

select add_platform.*
from add_platform
{%- if is_incremental() %}
{% if platform_dbt_processed_at %}
where 
  -- ensure that any new platform is included
  platform not in (
    {%- for platform in platform_dbt_processed_at['platform'] %}'{{ platform }}'
    {% if not loop.last %},{% endif -%}
    {%- endfor -%})
  -- for each existing platform get the incremental timestamp specific to that platform
  {%- for index in range(platform_dbt_processed_at['platform'] | length) %}
  or (
    platform = '{{ platform_dbt_processed_at["platform"][index] }}'
    and dbt_processed_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
  )
{% endfor %}
{% endif %}
{%- endif %}
