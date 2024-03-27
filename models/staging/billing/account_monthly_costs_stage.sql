with

costs as (

  select * from {{ ref('airbyte_costs_stage') }}

)

, payments as (

  select distinct
    account_id
    , date_trunc('month', received_at) as month  -- noqa: L029
  from {{ ref('fct_account_payments') }}

)

select
  costs.month as month  -- noqa: L029
  , payments.account_id
  , (
    costs.content_costs
    + costs.ad_costs
    + costs.fastly_edgecast
    + costs.gracenote
    + costs.stripe_chargebee
    + costs.taskus_costs_per_user
    + costs.aws_costs_per_user
  )           as backend_costs
from costs
join payments on (costs.month::date = payments.month::date)
