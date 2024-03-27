{{
  config(
    materialized='view'
  )
}}

with

braze_experiments as (

  select *
  from
    {{ ref('braze_experiment_users_stage') }}

)

, users as (

  select * from {{ ref('dim_users') }}

)

, accounts as (

  select * from {{ ref('dim_accounts') }}

)

select braze_experiments.*
from braze_experiments
left join users on (braze_experiments.user_id = users.user_id)
left join accounts on (users.account_id = accounts.account_id)
/*
  include accounts that are considered billable or any null (anonymous only) users
  exclude known non-billable accounts
*/
where coalesce(accounts.is_billable, true) is true