with

payment_events as (

  {{ dbt_utils.union_relations(
     relations=[
         ref('rails_prod_payment_succeeded_stage')
       , ref('rails_prod_payment_refunded_stage')
     ],
     include=[
         "account_id"
       , "received_at"
       , "packages"
       , "subscriber_state"
       , "subscriber_billing"
       , "amount"
       , "list_price"
       , "is_gift"
       , "promotion"
       , "rev_share_partner"
       , "is_active"
       , "event"
     ]
    ) 
  }}

)

, sequential_events as (

  select
    payment_events.*
    , lead(event) over (partition by account_id order by received_at) as next_event_type
  from payment_events

)

, identify_refunds as (

  select
    account_id
    , received_at
    , packages
    , subscriber_state
    , subscriber_billing
    , amount
    , list_price
    , is_gift
    , promotion
    , rev_share_partner
    , is_active
    , event
    , next_event_type
    , coalesce(
      event = 'payment_succeeded'
      and next_event_type = 'payment_refunded', false
    ) as is_refunded_payment
  from sequential_events

)

select
  account_id
  , received_at
  , packages
  , subscriber_state
  , subscriber_billing
  , amount
  , list_price
  , is_gift
  , promotion
  , rev_share_partner
  , is_active
  , row_number() over (
    partition by account_id
    order by
      received_at asc nulls last
  ) as payment_number
from identify_refunds
where event = 'payment_succeeded'
  and is_refunded_payment is false
