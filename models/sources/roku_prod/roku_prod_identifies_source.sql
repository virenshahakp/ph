with

source as (

  select * from {{ source('roku_prod', 'identifies') }}

)

, renamed as (

  select
    anonymous_id
    , user_id
    -- EM: when FAST phase 1.5 goes out, add:
    -- , device_id
    , context_device_advertising_id
    , received_at
    , "timestamp"
    , uuid_ts as loaded_at
  from source

)

select * from renamed
