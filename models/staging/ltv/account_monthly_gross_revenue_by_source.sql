{{ 
  config(
    materialized="ephemeral"
  )
}}

with

ad_revenue as (

  select * from {{ ref('daily_ad_revenue_per_user') }}

)

, subscription_revenue as (

  select * from {{ ref('subscriber_billing_rev_share') }}

)

select
  account_id
  , date_trunc('month', partition_date)  as month  -- noqa: L029
  , 'ad_revenue'                         as revenue_source
  , sum(coalesce(daily_ad_revenue, 0.0)) as revenue
from ad_revenue
where partition_date between date_trunc('month', '{{ var("philo_start_date") }}'::date) and current_date
{{ dbt_utils.group_by(n=3) }}

union all

select
  account_id
  , month
  , 'subscription_fees' as revenue_source
  , sum(amount::float)  as revenue
from subscription_revenue
{{ dbt_utils.group_by(n=3) }}