with

subscription_started as (

  select * from {{ source('rails_prod', 'subscription_started') }}

)

select
  user_id                                     as account_id
  , subscriber_state                          as subscriber_state
  , bulk                                      as bulk
  , received_at                               as received_at
  , event                                     as event
  , "timestamp"                               as subscribed_at
  , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
from subscription_started
