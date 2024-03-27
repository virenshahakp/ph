{{
  config(
    materialized='table'
    , dist='account_id'
    , sort=['created_at']
  )
}}

with

account_created as (

  select
    user_id
    , event_timestamp as created_at
  -- EM: for FAST Phase 1.5, excluding FAST-only account created events. Will revisit in next phase of FAST.
  from {{ ref('rails_prod_account_created_fast_stage') }}
  where user_id is not null

)

, credential_prompt as (

  select
    user_id
    , min(event_timestamp) as prompted_at
  from {{ ref('roku_prod_screens_stage') }}
  where lower(screen_name) = 'credentialprompt'
  group by 1

)

, user_credentialed as (

  select
    user_id
    , event_timestamp as credentialed_at
  from {{ ref('rails_prod_user_credentialed_stage' ) }}

)

, subscribed as (

  select
    account_id
    , min(subscribed_at) as first_subscribed_at
  from {{ ref('rails_prod_subscription_started_stage') }}
  group by 1

)

, paid as (

  select
    account_id
    -- first_payment may deviate from "finance" first payment, which is the first non-refunded payment
    , min(first_paid_at) as first_paid_at
  from {{ ref('rails_prod_payment_succeeded_stage') }}
  group by 1

)


select
  account_created.user_id             as account_id
  , account_created.created_at
  , credential_prompt.prompted_at     as first_prompted_at
  , user_credentialed.credentialed_at as credentialed_at
  , subscribed.first_subscribed_at    as first_subscribed_at
  , paid.first_paid_at                as first_paid_at
from account_created
left join credential_prompt
  on account_created.user_id = credential_prompt.user_id
left join user_credentialed
  on account_created.user_id = user_credentialed.user_id
left join subscribed
  on account_created.user_id = subscribed.account_id
left join paid
  on account_created.user_id = paid.account_id

