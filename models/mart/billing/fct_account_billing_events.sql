{{ config(materialized="table", dist="account_id", sort="received_at") }}
with

all_billing_events as (

  {{ dbt_utils.union_relations(
      relations=[
       ref('account_cancelled_never_paid')
        , ref('account_first_payment_refunded')
        , ref('account_first_payment_succeeded')
        , ref('bby_billing_prod_payment_succeeded')
        , ref('bby_billing_prod_subscription_cancelled')
        , ref('rails_prod_cancellation_complete_stage')
        , ref('rails_prod_cancellation_scheduled_stage')
        , ref('rails_prod_payment_failed_stage')
        , ref('rails_prod_payment_succeeded_stage')
        , ref('rails_prod_subscription_started_stage')
        , ref('rails_prod_trial_blocked_stage')
        , ref('rails_prod_trial_lapsed_stage')
        , ref('rails_prod_trial_started_stage')
      ]
      , include=[
        "account_id"
        , "event"
        , "amount"
        , "subscriber_billing"
        , "packages"
        , "received_at"
      ]
    )
  }}

)

select
  account_id
  , event
  , amount
  , received_at
  , packages
  -- for events that have no biller use 'unreported'
  , coalesce(subscriber_billing, 'unreported') as subscriber_billing
from all_billing_events
where
  received_at >= '2017-11-14'