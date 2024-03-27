with

dialog_displayed as (

  select * from {{ source('ios_prod', 'dialog_displayed') }}

)

, renamed as (

  select
    id                           as event_id
    , event                      as dialog_event
    , type
    , view
    , received_at
    , context_device_id
    , "timestamp"                as event_timestamp
    , uuid_ts                    as loaded_at
    , lower(anonymous_id)        as anonymous_id
    , lower(user_id)             as user_id
    , lower(context_instance_id) as context_instance_id
  from dialog_displayed
  where
    environment_analytics_version >= 24
)

select * from renamed
