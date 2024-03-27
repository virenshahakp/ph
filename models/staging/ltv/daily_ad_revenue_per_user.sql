{{ 
  config(
    materialized='incremental'
    , sort=['partition_date','account_id']
    , dist='account_id'
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , unique_key=['partition_date']
  )
}}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with

users as (

  select * from {{ ref('dim_users') }}

)

, daily_ad_rev as (

  select *
  from {{ ref('fct_ad_revenue_sutured_pid') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'

)

select
  daily_ad_rev.partition_date
  , users.account_id
  , daily_ad_rev.user_id
  , sum(daily_ad_rev.ad_revenue_dollars) as daily_ad_revenue
from daily_ad_rev
join users on (daily_ad_rev.user_id = users.user_id)
{{ dbt_utils.group_by(n=3) }}
