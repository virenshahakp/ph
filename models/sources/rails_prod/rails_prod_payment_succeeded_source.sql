with

payments as (

  select * from {{ source('rails_prod', 'payment_succeeded') }}

)

, renamed as (

  select
    id                                                       as payment_succeeded_id
    , user_id                                                as account_id
    , packages                                               as packages
    , subscriber_state                                       as subscriber_state
    , rev_share_partner                                      as rev_share_partner
    , true                                                   as is_active
    , event                                                  as event -- noqa: L029
    , received_at                                            as received_at
    , "timestamp"                                            as "timestamp"
    , promotion                                              as promotion
    , coalesce(subscriber_billing, 'chargebee')              as subscriber_billing
    , coalesce(amount_cents / 100.00, amount)                as amount
    , coalesce(is_gift is true and promotion is null, false) as is_gift
  from payments
  /*
  exclude falsely generated amazon payment succeeded events
  any event with subscriber_billing = 'amazon' and state = 'deactivated'
  should not have been sent
  */
  where
    coalesce(subscriber_billing, 'chargebee') != 'amazon' -- coalesce to include NULL biller
    or subscriber_state != 'deactivated'

)

select * from renamed
