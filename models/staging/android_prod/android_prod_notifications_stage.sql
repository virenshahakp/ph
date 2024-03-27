{{
  config(
    materialized='incremental'
    , unique_key='message_id'
    , dist='message_external_id' 
    , enabled=false
  )
}}

with

all_message_types as (

  -- note that chromecast is intentionally excluded since it does not generate interacts
  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_message_received_stage')
        , ref('android_prod_message_opened_stage')
        , ref('android_prod_message_clicked_stage')
        , ref('android_prod_message_dismissed_stage')
      ]
    )
  }}

)

{%- if is_incremental() %}

  , max_message_time as (

    select
      message_event
      , max(dbt_processed_at) as max_dbt_processed_at
    from {{ this }}
    {{ dbt_utils.group_by(n=1) }}

  )

{%- endif %}

select all_message_types.*
from all_message_types

{%- if is_incremental() %}

  left join max_message_time on all_message_types.message_event = max_message_time.message_event 
  where all_message_types.dbt_processed_at > max_message_time.max_dbt_processed_at

{%- endif %}
