{{ config(
    materialized='table'
    , dist='ALL'
    , sort='event_date'
) }}

with 

watched_minutes as (

  select 
    date_trunc('day', timestamp_start::timestamp)::date as event_date
    , minutes / 60.0 as hours
  from {{ ref('fct_watched_minutes') }}

)

, ad_revenue as (

  select
    impression_date::date as event_date
    , revenue
  from {{ ref('fct_ad_revenue') }}

)

, viewership as (

  select
    event_date
    , sum(hours) as hours_watched
  from 
    watched_minutes
  group by 1

)

select
  ad_revenue.event_date
  , viewership.hours_watched
  , sum(ad_revenue.revenue) / nullif(viewership.hours_watched, 0)::float as ad_revenue_per_hour_watched
from
  ad_revenue
left join viewership on (viewership.event_date = ad_revenue.event_date)
{{ dbt_utils.group_by(n=2) }}