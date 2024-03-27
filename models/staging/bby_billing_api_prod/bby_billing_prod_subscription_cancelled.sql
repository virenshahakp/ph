with

cancelled as (

  select * from {{ ref('bby_billing_api_prod_cancelled_stage') }}

)

, bestbuy_identifies as (

  select * from {{ ref('bby_billing_api_prod_identifies_stage') }}

)

, subscription_cancelled as (

  select
    bestbuy_identifies.account_id         as account_id
    , cancelled.received_at               as received_at
    , cancelled.end_date                  as end_date
    , cancelled.cancellation_effective_at as cancellation_effective_at
    , cancelled.cancel_reason_code        as cancel_reason_code
    , cancelled.packages                  as packages
    , 'bestbuy'                           as subscriber_billing
    , 'cancellation_complete'             as event
    , case
      when cancel_reason_code = 100 then 'deactivated'
      when cancel_reason_code = 200 and end_date > current_date then 'departing'
      when cancel_reason_code = 200 then 'deactivated'
      when cancel_reason_code = 210 and cancellation_effective_at > current_date then 'delinquent_access'
      when cancel_reason_code = 210 and end_date > current_date then 'delinquent_no_access'
      when cancel_reason_code = 210 then 'deactivated'
    end                                   as subscriber_state
  from cancelled
  join bestbuy_identifies on (cancelled.anonymous_id = bestbuy_identifies.anonymous_id)

)

select * from subscription_cancelled
