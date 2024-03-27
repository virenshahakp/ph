with

favorite as (

  select * from {{ source('dataserver_prod','channel_favorite') }}
)

, renamed as (

  select
    user_id       as user_id
    , channel_id  as channel_id
    , "timestamp" as event_timestamp
  from favorite

)

select * from renamed
