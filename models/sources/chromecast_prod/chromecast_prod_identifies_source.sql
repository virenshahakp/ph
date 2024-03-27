with

source as (

  select * from {{ source('chromecast_prod', 'identifies') }}

)

, renamed as (

  select
    anonymous_id  as anonymous_id
    , user_id     as user_id
    , received_at as received_at
    , player_uuid as context_device_id
    , "timestamp" as "timestamp"
    , uuid_ts     as loaded_at
  from source

)

select * from renamed
