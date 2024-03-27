with

identifies as (

  select * from {{ source('tvos_prod', 'identifies') }}

)

, renamed as (

  select
    received_at
    , "timestamp"
    , uuid_ts                  as loaded_at
    , lower(anonymous_id)      as anonymous_id
    , lower(user_id)           as user_id
    , lower(context_device_id) as context_device_id
  from identifies

)

select * from renamed
