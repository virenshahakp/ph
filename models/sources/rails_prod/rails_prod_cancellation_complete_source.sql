with

source as (

  select * from {{ source('rails_prod', 'cancellation_complete') }}

)

, renamed as (

  select
    id                                          as cancellation_complete_id
    , user_id                                   as account_id
    , packages                                  as packages
    , subscriber_state                          as subscriber_state
    , false                                     as is_active
    , event                                     as event
    , received_at                               as received_at
    , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
    -- COALESCE needed to correct data before May 2018
  from source

)

select * from renamed
