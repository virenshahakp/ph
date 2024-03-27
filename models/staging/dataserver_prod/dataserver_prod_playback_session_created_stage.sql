{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort='loaded_at'
    , on_schema_change='append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

playback_session as (

  select
    *
    , split_part(playback_session_id, '-', 1) as platform
  from {{ ref('dataserver_prod_playback_session_created_source') }}
  {%- if target.name != 'prod' %}
    where session_created_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
  {%- endif %}

)

select * from playback_session
where 1 = 1
  {%- if is_incremental() %}
    and loaded_at > {{ max_loaded_at }}
  {%- endif %}
-- if there are duplicate playback_session_ids in the records being processed only keep one of them
qualify row_number() over (partition by playback_session_id order by loaded_at desc, session_created_at desc) = 1