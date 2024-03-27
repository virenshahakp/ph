with

unsaves as (

  select * from {{ source('dataserver_prod','show_unsave') }}

)

, renamed as (

  select
    user_id
    , show_id
    , sent_at
    , received_at
    , "timestamp"
  from unsaves

)

select * from renamed