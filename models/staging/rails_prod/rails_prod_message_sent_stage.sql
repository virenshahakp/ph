{{
  config(
    materialized='incremental'
    , unique_key='message_id'
    , sort='loaded_at'
    , dist='message_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

message_sent as (

  select
    user_id
    , message_external_id
    , message_event
    , message_channel
    , message_name
    , message_type
    , braze_campaign_id
    , braze_canvas_id
    , braze_step_id
    , braze_variant_id
    , answers
    , message_id
    , loaded_at
    , event_timestamp
    , sysdate as dbt_processed_at
  from {{ ref('rails_prod_message_sent_source') }}

)

select * from message_sent
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}