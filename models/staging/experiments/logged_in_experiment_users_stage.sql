{{
  config(
    materialized='incremental'
    , dist='user_id'
    , sort=['event_timestamp', 'experiment_name', 'dbt_processed_at']
  )
}}

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}

with

user_experiments as (

  select
    anonymous_id       as anonymous_id
    , user_id          as user_id
    , experiment_name  as experiment_name
    , variant          as variant
    , event_timestamp  as event_timestamp
    , dbt_processed_at as dbt_processed_at
  from {{ ref('growthbook_logged_in_users_stage') }}
  {% if is_incremental() %}
    where dbt_processed_at > {{ max_processed_at }}
  {% endif %}

)

, users as (

  select * from {{ ref('dim_users') }}

)

, accounts as (

  select * from {{ ref('dim_accounts') }}

)

, acquisition_funnel as (

  select * from {{ ref('fct_acquisition_funnel') }}

)

, message_sent as (

  select *
  from {{ ref('rails_prod_message_sent_stage') }}

)

, message_sent_to_client as (

  select *
  from {{ ref('dataserver_prod_message_sent_to_client_stage') }}

)

, paid_access_ranges as (

  select *
  from {{ ref('fct_account_paid_access_ranges') }}

)

, fct_account_payments as (

  select *
  from {{ ref('fct_account_payments') }}

)

, all_user_experiments_prelim as (

  select
    user_experiments.anonymous_id
    , user_experiments.user_id
    , user_experiments.experiment_name
    , user_experiments.variant
    /*
      this timestamp is from our backend systems, but the timestamp to use
      for all experiments is coalesced from backend and message sent events
      to account for different types of experiments
    */
    , user_experiments.event_timestamp                                            as backend_event_timestamp
    , users.account_id

    -- generate user experiment dimensions
    , coalesce(web_funnel.visit_platform, rails.visit_platform)                   as visit_platform
    , coalesce(web_funnel.visit_platform_type, rails.visit_platform_type)         as visit_platform_type
    , coalesce(web_funnel.visit_platform_os, rails.visit_platform_os)             as visit_platform_os
    , coalesce(web_funnel.visit_browser, rails.visit_browser)                     as visit_browser
    , coalesce(web_funnel.signup_platform, rails.signup_platform)                 as signup_platform
    , coalesce(web_funnel.subscriber_billing, rails.subscriber_billing)           as subscriber_billing
    , coalesce(web_funnel.context_campaign_source, rails.context_campaign_source) as context_campaign_source
    , coalesce(web_funnel.context_campaign_name, rails.context_campaign_name)     as context_campaign_name
    , coalesce(web_funnel.context_page_path, rails.context_page_path)             as context_page_path
    , case
      when user_experiments.user_id is null then 'New User'
      when coalesce(
          message_sent_to_client.event_timestamp, message_sent.event_timestamp, user_experiments.event_timestamp
        ) >= accounts.trial_ended_at then 'User Post Trial'
      when coalesce(
          message_sent_to_client.event_timestamp, message_sent.event_timestamp, user_experiments.event_timestamp
        ) < accounts.trial_ended_at then 'User In Trial'
      else 'Unknown User State'
    end                                                                           as new_or_existing
    -- prioritize server defined message sent timestamps when available
    , coalesce(
      message_sent_to_client.event_timestamp, message_sent.event_timestamp, user_experiments.event_timestamp
    )                                                                             as event_timestamp
    , sysdate                                                                     as dbt_processed_at
  from user_experiments
  left join users on (user_experiments.user_id = users.user_id)
  left join accounts on (users.account_id = accounts.account_id)
  left join
    acquisition_funnel as web_funnel
    on (user_experiments.anonymous_id = web_funnel.visit_anonymous_id and user_experiments.anonymous_id is not null)
  left join acquisition_funnel as rails on (user_experiments.user_id = rails.account_id)
  left join
    message_sent
    on (user_experiments.user_id = message_sent.user_id and user_experiments.variant = message_sent.braze_canvas_id)
  left join message_sent_to_client on (message_sent.message_external_id = message_sent_to_client.message_external_id)

)

, payment_counts as (
  select
    all_user_experiments_prelim.account_id
    , all_user_experiments_prelim.event_timestamp
    /*
      Refunded payments are removed (maybe after a few days) from fct_account_payments for first payments
      not sure if refunded payments are removed for subsequent payments so this might not be 100 percent accurate
      another inaccuracy will occur if user added an ad-on off-cylce (then this will not reflect the number of *monthly* payments)
      could use fct_account_payments.packages field to exclude add-on only payments but the meaning of this field's values are not clear
      also, we are moving towards consolidating billing so maybe fewer add-on only payments going forward
    */
    , sum(case when fct_account_payments.amount > 0 then 1 else 0 end) as prior_payment_count
  from all_user_experiments_prelim
  left join fct_account_payments
    on (
      all_user_experiments_prelim.account_id = fct_account_payments.account_id
      and all_user_experiments_prelim.event_timestamp > fct_account_payments.received_at
    )
  {{ dbt_utils.group_by(n=2) }}
)

, all_user_experiments as (
  select
    all_user_experiments_prelim.*
    , payment_counts.prior_payment_count        as prior_payment_count
    , paid_access_ranges.account_id is not null as is_paid_access
    , case when all_user_experiments_prelim.new_or_existing = 'New User' then 'New User'
      when all_user_experiments_prelim.new_or_existing = 'User In Trial' then 'In Trial'
      when paid_access_ranges.account_id is not null and prior_payment_count = 1 then 'In First Payment Period'
      when paid_access_ranges.account_id is not null and prior_payment_count > 1 then 'In Subsequent Payment Period'
      -- users who have not made any payments, who have a trial end date that has expired and who have paid content access
      when paid_access_ranges.account_id is not null and all_user_experiments_prelim.new_or_existing = 'User Post Trial' then 'Post Trial Paid Content Access'
      -- TODO: verify this logic for FAST using the fact_access_ranges table, once this table is ready for use
      else 'FAST-only'
    end                                         as account_tenure
  from all_user_experiments_prelim
  left join paid_access_ranges
    on (
      all_user_experiments_prelim.account_id = paid_access_ranges.account_id
      and all_user_experiments_prelim.event_timestamp between paid_access_ranges.date_range_start_at and
      paid_access_ranges.date_range_end_at
    )
  left join payment_counts on (
    all_user_experiments_prelim.account_id = payment_counts.account_id
    and all_user_experiments_prelim.event_timestamp = payment_counts.event_timestamp
  )

)

select * from all_user_experiments
