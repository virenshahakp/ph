{{
  config(
    materialized='incremental'
    , dist='message_id' 
    , sort=['event_timestamp', 'dbt_processed_at', 'platform']
    , enabled=true
  )
}}

with

all_notifications as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('all_platforms_message_sent_stage')
        , ref('all_platforms_message_sent_to_client_stage')
        , ref('all_platforms_message_received_stage')
        , ref('all_platforms_message_clicked_stage')
        , ref('all_platforms_message_opened_stage')
        , ref('all_platforms_message_dismissed_stage')
      ]
      , exclude=[
        "_dbt_source_relation"
        , "braze_campaign_id"
        , "braze_canvas_id"
        , "braze_step_id"
        , "braze_variant_id"]
    )
  }}

)

, notification_properties as (

  select
    message_external_id
    , message_id
    , braze_campaign_id
    , braze_canvas_id
    , braze_step_id
    , braze_variant_id
    , message_name
  from {{ ref('all_platforms_message_sent_stage') }}

)

{%- if is_incremental() %}

  , max_event_time as (

    select
      message_event
      , max(dbt_processed_at)                        as max_dbt_processed_at
    from {{ this }}
    {{ dbt_utils.group_by(n=1) }}

  ) 

{%- endif %}

select
  all_notifications.user_id
  , all_notifications.message_id
  , all_notifications.message_event
  , all_notifications.message_external_id
  , all_notifications.message_channel
  , all_notifications.event_timestamp
  , all_notifications.platform
  , notification_properties.braze_campaign_id
  , notification_properties.braze_canvas_id
  , notification_properties.braze_step_id
  , notification_properties.braze_variant_id
  , notification_properties.message_name
  , sysdate                                     as dbt_processed_at
from all_notifications
left join notification_properties on notification_properties.message_external_id = all_notifications.message_external_id

{%- if is_incremental() %}

  left join max_event_time on all_notifications.message_event = max_event_time.message_event 
  where all_notifications.dbt_processed_at > max_event_time.max_dbt_processed_at
    or max_event_time.message_event is null 

{%- endif %}