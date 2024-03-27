{{ config(materialized='table', sort='observed_month', dist='account_id' ) }}
with

paid_range as (

  select * from {{ ref('fct_account_paid_access_ranges') }}

)

, first_paid as (

  select
    account_id
    , min(date_range_start_at)                      as first_paid_at
    , min(date_trunc('month', date_range_start_at)) as initial_cohort
    , max(
      datediff(
        'month'
        , date_trunc('month', date_range_start_at)
        , date_trunc('month', current_date)
      )
    )                                               as max_billing_periods
  from paid_range
  {{ dbt_utils.group_by(n=1) }}

)

, month_spine as (

  select distinct
    date_trunc('month', observation_date)                                                        as observed_month
    , coalesce(date_trunc('month', observation_date) = date_trunc('month', current_date), false) as is_current_period
  from {{ ref('dim_dates') }}
  where observation_date <= current_date

)

, paid_users as (

  select
    *
    , date_trunc('month', date_range_start_at) as start_month
  from paid_range
  -- only active subscriptions or subscriptions that lasted more than 2 days
  -- this handles some immediate cancellations and biller switching behaviors
  -- that do not reflect true paid subscription periods
  where is_active is true
    or datediff('days', date_range_start_at::timestamp, date_range_end_at::timestamp) > 2

)

, primary_platform as (

  select
    account_id
    , platform
  from {{ ref('fct_account_platforms') }}
  where platform_streams_rank = 1

)

, acquisition_biller as (

  select
    fct_acquisition_funnel.account_id
    , fct_acquisition_funnel.subscriber_billing
    , fct_user_signed_up_all_sources.platform
    , row_number() over (
      partition by fct_acquisition_funnel.account_id
      order by coalesce(fct_user_signed_up_all_sources.signed_up_at, fct_acquisition_funnel.visited_at) asc nulls last
    ) as event_order
  from {{ ref('fct_acquisition_funnel') }}
  left join
    {{ ref('fct_user_signed_up_all_sources') }}
    on (fct_acquisition_funnel.account_id = fct_user_signed_up_all_sources.user_id)

)

, user_active_periods as (

  select
    paid_users.account_id
    , first_paid.initial_cohort
    , month_spine.observed_month
    , first_paid.max_billing_periods
    , paid_users.date_range_start_at
    , paid_users.date_range_end_at
    , month_spine.is_current_period
    , paid_users.subscriber_billing_start                          as subscriber_billing
    , date_trunc('month', paid_users.date_range_start_at)          as resurrected_cohort
    , coalesce(acquisition_biller.subscriber_billing, 'chargebee') as acquisition_biller
    , coalesce(primary_platform.platform, 'unknown')               as primary_platform
    , coalesce(acquisition_biller.platform, 'unknown')             as signup_platform
    , datediff(
      'month'
      , first_paid.initial_cohort
      , month_spine.observed_month
    )                                                              as elapsed_billing_periods
    , datediff(
      'month'
      , date_trunc('month', paid_users.date_range_start_at)
      , month_spine.observed_month
    )                                                              as resurrected_elapsed_billing_periods
    , count(1) over (
      partition by paid_users.account_id
      order by paid_users.date_range_start_at rows unbounded preceding
    )                                                              as bill_number
    , count(1) over (
      partition by paid_users.account_id, date_trunc('month', paid_users.date_range_start_at)
      order by paid_users.date_range_start_at rows unbounded preceding
    )                                                              as resurrected_bill_number
  from paid_users
  left join first_paid on (paid_users.account_id = first_paid.account_id)
  join month_spine on (month_spine.observed_month between date_trunc('month', paid_users.date_range_start_at) and paid_users.date_range_end_at)
  left join primary_platform on (paid_users.account_id = primary_platform.account_id)
  left join acquisition_biller on (
    paid_users.account_id = acquisition_biller.account_id
    and acquisition_biller.event_order = 1
  )

)

select * from user_active_periods