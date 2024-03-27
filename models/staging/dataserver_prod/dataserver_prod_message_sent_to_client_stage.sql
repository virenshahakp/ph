-- message_id is the unique key for product, as there are duplicates in message_external_id in raw data

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

message_sent_to_client as (

  select
    user_id
    , message_id
    , message_external_id
    , message_event
    , loaded_at
    , event_timestamp
    , sysdate as dbt_processed_at
  from {{ ref('dataserver_prod_message_sent_to_client_source') }}
  order by event_timestamp

)

select * from message_sent_to_client
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}