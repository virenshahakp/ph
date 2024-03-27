{{
  config(
    materialized='incremental'
    , unique_key='message_id'
    , sort='dbt_processed_at'
    , dist='message_id' 
  )
}}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value('dbt_processed_at') 
%}

with

sent_messages_to_client as (

-- performing an empty union relations to create column _dbt_source_relation that gets called in the notification_funnel
  {{ dbt_utils.union_relations(
      relations=[
        ref('dataserver_prod_message_sent_to_client_stage')
      ]
    )
  }}

)

, add_platform as (

  select
    sent_messages_to_client.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from sent_messages_to_client

)

select add_platform.*
from add_platform
{%- if is_incremental() %}
{% if platform_dbt_processed_at %}
where 
  -- ensure that any new platform is included
  platform not in (  
    {%- for platform in platform_dbt_processed_at['platform'] %}
    '{{ platform }}'  
    {% if not loop.last %},{%- endif -%}
    {%- endfor -%}
  )
  -- for each existing platform get the incremental timestamp specific to that platform
  {% for index in range(platform_dbt_processed_at['platform'] | length) %}
  or (
    platform = '{{ platform_dbt_processed_at["platform"][index] }}'
    and loaded_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
  )
{% endfor %}
{%- endif %}
{%- endif %}
