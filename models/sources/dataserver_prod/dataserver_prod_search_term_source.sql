with

searches as (

  select * from {{ source('dataserver_prod','search_term') }}

)

, renamed as (

  select
    user_id
    , term
    , user_agent
    , received_at
    , "timestamp" as search_at
  from searches

)

select * from renamed
