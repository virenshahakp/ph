with

launches as (

  select * from {{ source('chromecast_prod', 'launch') }}

)

, renamed as (

  select
    id                       as launch_id
    , anonymous_id           as anonymous_id
    , user_id                as user_id
    , player_uuid            as context_device_id
    , "timestamp"            as "timestamp"
    , nullif(context_ip, '') as context_ip
  from launches
  where anonymous_id is not null

)

select * from renamed
