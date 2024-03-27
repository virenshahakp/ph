with

identifies as (

  select * from {{ source('ios_prod', 'identifies') }}

)

, renamed as (

  select
    received_at
    , context_device_id
    , "timestamp"
    , uuid_ts             as loaded_at
    , lower(anonymous_id) as anonymous_id
    , lower(user_id)      as user_id
  from identifies

)

select * from renamed
