{{
  config(
    materialized='view'
  )
}}

with

accounts as (

  select * from {{ ref('dim_accounts') }}

)

, users as (

  select * from {{ ref('dim_users') }}

)

, anon_xp_users as (

  select * from {{ ref('anonymous_experiment_users_stage') }}

)

select anon_xp_users.*
from anon_xp_users
left join users on (anon_xp_users.user_id = users.user_id)
left join accounts on (users.account_id = accounts.account_id)
/*
  include accounts that are considered billable or any null (anonymous only) users
  exclude known non-billable accounts
*/
where coalesce(accounts.is_billable, true) is true