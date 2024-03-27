{{
  config(
    materialized='incremental'
    , sort='dbt_processed_at'
    , dist='user_id' 
  )
}}

{%- set event_type_dbt_processed_at =
  incremental_max_event_type_value('loaded_at', '_dbt_source_relation') 
%}

with

user_experiments as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('dataserver_prod_experiment_viewed_stage')
        , ref('web_prod_experiment_users_stage')
        , ref('rails_prod_experiment_users_stage')
      ]
    )
  }}

)

-- the incremental loops are causing sqlfluff parsing errors, so disable linting of the incremental block
-- noqa: disable=all
select 
  user_experiments.*
  , sysdate as dbt_processed_at
from user_experiments
{%- if is_incremental() %}
where 
  -- ensure that any new event_type is included
  _dbt_source_relation not in (
    {%- for event_type in event_type_dbt_processed_at['_dbt_source_relation'] %}
    '{{ event_type }}'
    {% if not loop.last %}, {% endif -%}
    {%- endfor -%})
  -- for each existing event_type get the incremental timestamp specific to that event_type
  {%- for index in range(event_type_dbt_processed_at['_dbt_source_relation'] | length) %}
  or (
    _dbt_source_relation = '{{ event_type_dbt_processed_at["_dbt_source_relation"][index] }}'
    and loaded_at > '{{ event_type_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
  )
  {% endfor %}
{%- endif %}
