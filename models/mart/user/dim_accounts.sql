{{
  config(
    materialized='incremental'
    , unique_key='account_id'
    , sort=['is_billable', 'trial_started_at']
    , dist='account_id'
  )
}}

with

demographics as (

  select * from {{ ref('account_demographics_stage') }}

)

, payments as (

  select * from {{ ref('fct_account_payments') }}

)

, refunds as (

  select * from {{ ref('rails_prod_payment_refunded_stage') }}

)

, trials as (

  select * from {{ ref('rails_prod_trial_started_stage') }}

)

, dma_names as (

  select * from {{ ref('dma_code_name') }}

)

, payment_gift_promotion_status as (

  select
    account_id
    , amount
    , first_value(is_gift respect nulls) over (
      partition by account_id
      order by received_at asc
      rows between unbounded preceding and unbounded following
    ) as first_gift
    , last_value(is_gift respect nulls) over (
      partition by account_id
      order by received_at asc
      rows between unbounded preceding and unbounded following
    ) as last_gift
    , first_value(promotion respect nulls) over (
      partition by account_id
      order by received_at asc
      rows between unbounded preceding and unbounded following
    ) as first_promotion
    , last_value(promotion respect nulls) over (
      partition by account_id
      order by received_at asc
      rows between unbounded preceding and unbounded following
    ) as last_promotion
    , first_value(received_at) over (
      partition by account_id
      order by received_at asc
      rows between unbounded preceding and unbounded following
    ) as first_payment_at
  from payments

)

, trial_summary as (

  select
    account_id
    , min(trial_started_at)      as trial_started_at
    , max(overall_trial_ends_at) as trial_ended_at
  from trials
  {{ dbt_utils.group_by(n=1) }}

)

, payment_summary as (

  select
    account_id
    , first_gift
    , last_gift
    , first_promotion
    , last_promotion
    , first_payment_at
    , sum(amount) as total_paid
    , count(1)    as total_payments
  from payment_gift_promotion_status
  {{ dbt_utils.group_by(n=6) }}

)

, refund_summary as (

  select
    account_id
    , sum(amount) as total_refunded
    , count(1)    as total_refunds
  from refunds
  group by 1

)

select
  demographics.account_id
  , demographics.roles
  , demographics.subscriber_billing
  , demographics.subscriber_state
  , demographics.created_at
  , demographics.labels
  , demographics.packages
  , demographics.zip
  , demographics.dma_code
  , demographics.dma_region
  , dma_names.name                as dma_name
  , demographics.income
  , demographics.age_range
  , demographics.gender
  , demographics.referrer_id
  , demographics.referral_type
  , demographics.has_email
  , demographics.has_phone
  , demographics.is_direct_billed
  , demographics.is_direct_billed as direct_billed -- legacy chart support
  , demographics.signup_source
  , demographics.demographic_gender
  , demographics.demographic_homeowner
  , demographics.demographic_people_in_household
  , demographics.demographic_household_composition
  , demographics.demographic_education_level
  , demographics.demographic_income
  , demographics.demographic_age_range
  , demographics.user_state
  , demographics.is_billable
  , payment_summary.first_gift
  , payment_summary.last_gift
  , payment_summary.first_promotion
  , payment_summary.last_promotion
  , payment_summary.total_payments
  , payment_summary.total_paid
  , payment_summary.first_payment_at
  , refund_summary.total_refunds
  , refund_summary.total_refunded
  , payment_summary.total_paid    as net_paid -- legacy support in reports
  , trial_summary.trial_started_at
  , coalesce(
    trial_summary.trial_ended_at
    , demographics.created_at + interval '7 days'
  )                               as trial_ended_at
from demographics
left join dma_names on (demographics.dma_code = dma_names.code)
left join payment_summary on (demographics.account_id = payment_summary.account_id)
left join refund_summary on (demographics.account_id = refund_summary.account_id)
left join trial_summary on (demographics.account_id = trial_summary.account_id)
