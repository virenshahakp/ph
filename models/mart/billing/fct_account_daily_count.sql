{{
  config(
    materialized='incremental'
    , unique_key='observation_date'
    , sort='observation_date'
  )
}}

{%- set max_observation_date = incremental_max_value('observation_date') %}

with dates as (

  select observation_date from {{ ref('dim_dates') }}
  where observation_date <= dateadd('day', -1, current_date)

  -- incremental logic, including a lookback for 7 days to capture any late arriving data 
  {% if is_incremental() %}
    and observation_date > {{ max_observation_date }} - interval '7 days'
  {% endif %}

)

, dim_accounts as (

  select

    account_id
    , is_billable
    , trial_started_at
    , trial_ended_at

  from {{ ref('dim_accounts') }}

)

, paid_accounts as (

  select

    account_id
    , date_range_start_at
    , date_range_end_at

  from {{ ref('fct_account_paid_access_ranges') }}

  -- incremental logic, including a lookback for 7 days to capture any late arriving data
  {% if is_incremental() %}
    where date_range_end_at > {{ max_observation_date }} - interval '7 days'
  {% endif %}

)


, daily_sub_count_paid as (

  select

    dates.observation_date
    , count(paid_accounts.account_id) as num_accounts_paid

  from dates
  left join paid_accounts on dates.observation_date between paid_accounts.date_range_start_at and paid_accounts.date_range_end_at
  left join dim_accounts on paid_accounts.account_id = dim_accounts.account_id

  -- removing test accounts
  where dim_accounts.is_billable is not false

  {{ dbt_utils.group_by(n=1) }}

)

, daily_sub_count_trial as (

  select

    dates.observation_date
    , count(dim_accounts.account_id) as num_accounts_trial

  from dates -- left join on dates to capture any dates that have 0 paid subscribers
  left join dim_accounts on dates.observation_date between dim_accounts.trial_started_at and dim_accounts.trial_ended_at
  -- removing test accounts
  where dim_accounts.is_billable is not false
  -- incremental logic not implemented as dim_accounts_table is small enough to not impede performances
  {{ dbt_utils.group_by(n=1) }}
)

, daily_sub_count as (

  select

    daily_sub_count_paid.observation_date
    , daily_sub_count_paid.num_accounts_paid
    , daily_sub_count_trial.num_accounts_trial

  from
    daily_sub_count_paid
  left join daily_sub_count_trial
    on
      daily_sub_count_paid.observation_date = daily_sub_count_trial.observation_date
)

select *
from daily_sub_count