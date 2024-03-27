with

completed_events as (

  {{ dbt_utils.union_relations(
     relations=[
       ref('bby_billing_api_prod_modified_stage')
       , ref('bby_billing_prod_payment_succeeded')
       , ref('bby_billing_prod_subscription_cancelled')
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
       , "is_active"
       , "bby_price_type"
       , "cancel_reason_code"
       , "modify_reason_code"
       , "activated_at"
       , "end_date"
       , "cancellation_effective_at"
       , "modified_at"
     ]
    ) 
  }}

)

/*
 * modify events that are setting autorenew to off (code '04')
 * are effectively scheduled cancellations these could be
 * added to the models to match rails_prod scheduled cancellations
*/

, check_for_cancellation_modification as (

-- a modification event after a cancellation can undo or reverse the cancellation
  select
    completed_events.*
    , lead(modified_at) over (partition by account_id order by received_at)        as modify_lead
    , lead(modify_reason_code) over (partition by account_id order by received_at) as modify_reason_lead
  from completed_events
  where modify_reason_code is null
    or modify_reason_code in ('02', '03')

)

, reverse_cancellation as (

-- the is_modified identifies cancellations that have been reversed and should be ignored
  select
    *
    , coalesce(cancellation_effective_at > modify_lead, false) as is_modified
  from check_for_cancellation_modification

)

, scheduled_and_completed_cancellations as (

  select *
  from reverse_cancellation
  where is_modified is false
    and cancellation_effective_at is not null

)

, identify_unpaid_trials as (

  select
    account_id
    , case
      /*
          when the user pays something at the time of purchase (price_type = 0)
          we consider them a paid subscriber from the time of the redemption
        */
      when bby_price_type = 0
        then received_at
        /*
          when the user pays nothing (price_type = 1 or NULL) at the time of
          the bby transaction we consider them a paid subscriber one month after
          their activated event (day of purchase from BBY)
        */
      when cancellation_effective_at is not null
        then greatest(received_at, cancellation_effective_at)
      else coalesce( {{ dbt.dateadd('month', 1, 'activated_at') }}, received_at)
    end as received_at
    , activated_at -- this date determines when bby will bill the customer each month
    , packages
    , subscriber_state
    , subscriber_billing
    , case
      when bby_price_type = 0
        then amount
      else list_price
    end as amount
    , list_price
    , is_gift
    , is_active
  from reverse_cancellation
  where is_modified is false
    and modified_at is null

)

/*
    if a user cancels before the next event, in particular with price types that indicate
    that the user paid nothing, aka had a free trial period
    we need to remove the start/synthesized first payment that will no longer occur
    if they cancel before the payment is scheduled to take place
*/
, remove_subscriptions_cancelled_before_paid_start as (

  select
    identify_unpaid_trials.*
    , scheduled_and_completed_cancellations.cancellation_effective_at
  from identify_unpaid_trials
  left join scheduled_and_completed_cancellations on (identify_unpaid_trials.account_id = scheduled_and_completed_cancellations.account_id)
  where scheduled_and_completed_cancellations.cancellation_effective_at is null
    or scheduled_and_completed_cancellations.cancellation_effective_at >= identify_unpaid_trials.received_at

)

select *
from remove_subscriptions_cancelled_before_paid_start
-- remove the records that are for accounts still in an unpaid BBY trial period
-- as they have not yet paid
where received_at < current_date
