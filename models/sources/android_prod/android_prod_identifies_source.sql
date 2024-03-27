with

source as (

  select * from {{ source('android_prod', 'identifies') }}

)

, renamed as (

  select
    anonymous_id
    , user_id
    , context_device_id as device_id
    , received_at
    , "timestamp"
    , uuid_ts           as loaded_at
  from source

)

select * from renamed
