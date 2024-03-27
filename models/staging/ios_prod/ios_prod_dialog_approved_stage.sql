{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , sort = 'loaded_at'
    , dist='event_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

source as (

  select 
    event_id
    , user_id
    , context_instance_id
    , dialog_event
    , type
    , view
    , received_at
    , loaded_at
    , context_device_id
    , event_timestamp 
    , sysdate               as dbt_processed_at
  from {{ ref('ios_prod_dialog_approved_source') }}
  order by event_timestamp

)

select * from source

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}
{% endif %}
