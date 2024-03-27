with

source as (

  select * from {{ source('web_prod', 'identifies') }}

)

, renamed as (

  select
    anonymous_id
    , user_id
    , received_at
    , context_user_agent
    , "timestamp"
    , uuid_ts as loaded_at
  from source

)

select * from renamed
