{{
  config(
    materialized='incremental'
    , dist='user_id'
    , sort=['user_id', 'experiment_name', 'event_timestamp', 'dbt_processed_at']
  )
}}

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}


with

braze_experiments as (

  select *
  from
    {{ ref('growthbook_braze_experiment_users_stage') }}
  -- we have records for braze campaigns that were not run as experiments
  -- get rid of these as only experiments belong in the table we're building here
  where experiment_id is not null

)

, users as (

  select * from {{ ref('dim_users') }}

)

, accounts as (

  select * from {{ ref('dim_accounts') }}

)

, paid_access_ranges as (

  select *
  from {{ ref('fct_account_paid_access_ranges') }}

)

, fct_account_payments as (

  select *
  from {{ ref('fct_account_payments') }}

)

, experiment_start as (

  select
    experiment_id
    , min(event_timestamp) as experiment_started_at
  from braze_experiments
  {{ dbt_utils.group_by(n=1) }}

)

, users_account as (

  select
    users.user_id
    , accounts.account_id
    , coalesce(
      accounts.trial_ended_at
      , accounts.created_at + interval '7 days'
    ) as trial_ended_at
  from users
  join accounts on (users.account_id = accounts.account_id)

)

, message_sent as (

  select *
  from {{ ref('rails_prod_message_sent_stage') }}

)

, message_sent_to_client as (

  select *
  from {{ ref('dataserver_prod_message_sent_to_client_stage') }}

)


, all_braze_experiments_prelim as (

  select
    braze_experiments.*
    , users_account.account_id
    , case
      when braze_experiments.user_id is null then 'New User'
      when experiment_start.experiment_started_at >= users_account.trial_ended_at then 'User Post Trial'
      when experiment_start.experiment_started_at <= users_account.trial_ended_at then 'User In Trial'
      -- should only be in an Unknown User State if experiment_started_at or trial_ended_at timestamps are missing
      else 'Unknown User State'
    end as new_or_existing
    , coalesce(
      message_sent_to_client.event_timestamp, message_sent.event_timestamp, braze_experiments.event_timestamp
    )   as user_shown_experiment_timestamp
  from braze_experiments
  left join experiment_start on braze_experiments.experiment_id = experiment_start.experiment_id
  left join users_account on (braze_experiments.user_id = users_account.user_id)
  left join
    message_sent
    on
      braze_experiments.user_id = message_sent.user_id
      and braze_experiments.variant_id = message_sent.braze_canvas_id
  left join message_sent_to_client on message_sent.message_external_id = message_sent_to_client.message_external_id
  {%- if is_incremental() %}
    where
      braze_experiments.dbt_processed_at > {{ max_processed_at }}
  {%- endif %}

)

, payment_counts as (
  select
    all_braze_experiments_prelim.account_id
    , all_braze_experiments_prelim.event_timestamp
    /*
      Refunded payments are removed (maybe after a few days) from fct_account_payments for first payments
      not sure if refunded payments are removed for subsequent payments so this might not be 100 percent accurate
      another inaccuracy will occur if user added an ad-on off-cylce (then this will not reflect the number of *monthly* payments)
      could use fct_account_payments.packages field to exclude add-on only payments but the meaning of this field's values are not clear
      also, we are moving towards consolidating billing so maybe fewer add-on only payments going forward
    */
    , sum(case when fct_account_payments.amount > 0 then 1 else 0 end) as prior_payment_count
  from all_braze_experiments_prelim
  left join fct_account_payments
    on (
      all_braze_experiments_prelim.account_id = fct_account_payments.account_id
      and all_braze_experiments_prelim.event_timestamp > fct_account_payments.received_at
    )
  {{ dbt_utils.group_by(n=2) }}
)

, all_braze_experiments as (
  select
    all_braze_experiments_prelim.*
    , payment_counts.prior_payment_count        as prior_payment_count
    , paid_access_ranges.account_id is not null as is_paid_access
    , case when all_braze_experiments_prelim.new_or_existing = 'New User' then 'New User'
      when all_braze_experiments_prelim.new_or_existing = 'User In Trial' then 'In Trial'
      when paid_access_ranges.account_id is not null and prior_payment_count = 1 then 'In First Payment Period'
      when paid_access_ranges.account_id is not null and prior_payment_count > 1 then 'In Subsequent Payment Period'
      -- users who have not made any payments, who have a trial end date that has expired and who have paid content access
      when
        paid_access_ranges.account_id is not null and all_braze_experiments_prelim.new_or_existing = 'User Post Trial'
        then 'Post Trial Paid Content Access'
      -- TODO: verify this logic for FAST using the fact_access_ranges table, once this table is ready for use
      else 'FAST-only'
    end                                         as account_tenure
  from all_braze_experiments_prelim
  left join paid_access_ranges
    on (
      all_braze_experiments_prelim.account_id = paid_access_ranges.account_id
      and all_braze_experiments_prelim.event_timestamp between paid_access_ranges.date_range_start_at and
      paid_access_ranges.date_range_end_at
    )
  left join payment_counts on (
    all_braze_experiments_prelim.account_id = payment_counts.account_id
    and all_braze_experiments_prelim.event_timestamp = payment_counts.event_timestamp
  )

)

select
  all_braze_experiments.user_id
  , all_braze_experiments.account_id
  , all_braze_experiments.event_timestamp
  , all_braze_experiments.experiment_id
  , all_braze_experiments.experiment_name
  , all_braze_experiments.variant_id
  , all_braze_experiments.variant_name
  , all_braze_experiments.new_or_existing
  , all_braze_experiments.account_tenure
  , all_braze_experiments.prior_payment_count
  , all_braze_experiments.is_paid_access
  , all_braze_experiments.user_shown_experiment_timestamp
  , sysdate as dbt_processed_at
from all_braze_experiments
