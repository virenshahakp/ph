-- message_id is the unique key for product, as there are duplicates in message_external_id in raw data

{{
  config(
    materialized='incremental'
    , unique_key='message_id'
    , sort = 'loaded_at'
    , dist='message_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

source as (

  select
    user_id
    , message_external_id
    , message_event
    , message_channel
    , message_id
    , loaded_at
    , event_timestamp
    , sysdate as dbt_processed_at
  from {{ ref('web_prod_message_opened_source') }}
  order by event_timestamp

)

select * from source

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}
{% endif %}