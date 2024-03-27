{{
  config(
    materialized='incremental'
    , dist='message_id' 
    , sort=['event_timestamp', 'dbt_processed_at']
    , enabled=false
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

, all_notifications_clean as (

  select * from all_notifications
  where message_channel not in ('sms', 'voice')
    and message_name not in ('enjoy_philo', 'app_store_review')
    and user_id is not null
)

, notification_properties as (

  select
    message_external_id
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
      , max(dbt_processed_at)                  as max_dbt_processed_at
    from {{ this }}
    {{ dbt_utils.group_by(n=1) }}

  )

{%- endif %}


, notification_deduped as (

  select 
    user_id
    , platform
    , message_external_id
    , message_event
    , message_channel
    , message_id
    -- needs review
    -- last_value(message_id)
    -- over (partition by message_external_id 
    --   order by event_timestamp desc 
    --   rows between unbounded preceding and unbounded following) as message_id 
    , last_value(loaded_at)
    over (partition by message_external_id 
      order by event_timestamp asc 
      rows between unbounded preceding and unbounded following) as loaded_at 
    , last_value(event_timestamp) 
    over (partition by message_external_id 
      order by event_timestamp asc 
      rows between unbounded preceding and unbounded following) as event_timestamp 
    , sysdate                                                   as dbt_processed_at
  from all_notifications_clean
  where
    message_external_id is not null

)

, notifications_braze as (

  select
    notification_deduped.user_id
    , notification_deduped.message_id
    , notification_deduped.message_event
    , notification_deduped.message_external_id
    , notification_deduped.message_channel
    , notification_deduped.event_timestamp
    , notification_deduped.platform
    , notification_properties.braze_campaign_id
    , notification_properties.braze_canvas_id
    , notification_properties.braze_step_id
    , notification_properties.braze_variant_id
    , notification_properties.message_name
    , sysdate                                     as dbt_processed_at
  from notification_deduped
  left join notification_properties 
    on notification_properties.message_external_id = notification_deduped.message_external_id 

  {%- if is_incremental() %}

    left join max_event_time on notification_deduped.message_event = max_event_time.message_event 
    where notification_deduped.dbt_processed_at > max_event_time.max_dbt_processed_at
      or max_event_time.message_event is null 

  {%- endif %}

)

select distinct * from notifications_braze