with

saves as (

  select * from {{ source('dataserver_prod','show_save') }}

)

, renamed as (

  select
    user_id
    , show_id
    , sent_at
    , received_at
    , "timestamp"
  from saves

)

select * from renamed