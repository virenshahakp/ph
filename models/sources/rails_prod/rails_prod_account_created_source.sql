with source as (

  select *
  from {{ source('rails_prod', 'account_created') }}

)

, renamed as (

  select
    user_id         as user_id
    , "timestamp"   as event_timestamp
    , received_at   as received_at
    , signup_source as signup_source
  from source

)

select * from renamed
