with

source as (

  select * from {{ source('rails_prod', 'cancellation_scheduled') }}

)

, renamed as (

  select
    id                                          as cancellation_scheduled_id
    , user_id                                   as account_id
    , subscriber_state                          as subscriber_state
    , event                                     as event
    , received_at                               as received_at
    , "timestamp"                               as "timestamp"
    , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
  from source

)

select * from renamed
