with

identifies as (

  select * from {{ source('bby_billing_api_prod', 'identifies') }}

)

, renamed as (

  select
    anonymous_id  as anonymous_id
    , user_id     as account_id
    , received_at as received_at
    , "timestamp" as event_timestamp
  from identifies

)

select * from renamed
