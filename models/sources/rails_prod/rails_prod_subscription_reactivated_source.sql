with

reactivated_subscriptions as (

  select * from {{ source('rails_prod', 'subscription_reactivated') }}

)

, renamed as (

  select
    user_id                                     as account_id
    , subscriber_state                          as subscriber_state
    , received_at                               as received_at
    , event                                     as event
    , "timestamp"                               as reactivated_at
    , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
  from reactivated_subscriptions

)

select * from renamed
