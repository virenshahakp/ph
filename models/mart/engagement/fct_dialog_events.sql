{{
  config(
    materialized='incremental'
    , dist='event_id' 
    , sort=['event_timestamp', 'dbt_processed_at', 'platform']
    , enabled=true
  )
}}

{%- set dialog_event_dbt_processed_at =
  incremental_max_event_type_value('loaded_at', '_dbt_source_relation') 
%}

with

all_dialogs as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('all_platforms_dialog_approved_stage')
        , ref('all_platforms_dialog_dismissed_stage')
        , ref('all_platforms_dialog_displayed_stage')
      ]
      , exclude=[
        "_dbt_source_relation"
      ]
    )
  }}

)

select
  all_dialogs.event_id
  , all_dialogs.user_id
  , all_dialogs.context_instance_id
  , all_dialogs.dialog_event
  , all_dialogs.type
  , all_dialogs.view
  , all_dialogs.received_at
  , all_dialogs.loaded_at
  , all_dialogs.context_device_id
  , all_dialogs.event_timestamp
  , all_dialogs.platform
  , all_dialogs._dbt_source_relation
  , sysdate                                     as dbt_processed_at
from all_dialogs
{%- if is_incremental() %}
where 
  _dbt_source_relation not in (
    {%- for dialog_event in dialog_event_dbt_processed_at['_dbt_source_relation'] %}
    '{{ dialog_event }}'
    {% if not loop.last %}, {% endif -%}
    {%- endfor -%})
  {%- for index in range(dialog_event_dbt_processed_at['_dbt_source_relation'] | length) %}
  or (
    _dbt_source_relation = '{{ dialog_event_dbt_processed_at["_dbt_source_relation"][index] }}'
    and loaded_at > '{{ dialog_event_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
  )
{% endfor %}
{%- endif %}
