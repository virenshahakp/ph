{{ config(
    materialized='tuple_incremental'
    , sort=['report_timezone', 'report_time']
    , dist='adomain'
    , unique_key=['report_timezone', 'report_time'] 
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_schema_change = 'append_new_columns'
    
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with pre_agg as (
  select 
    utc_ts
    , is_paid
    , is_count
    , sov
    , bidder_name
    , network
    , bid_density
    , demand_partner
    , channel
    , sov__tier
    , coalesce(adomain, '') as adomain    
    , sum(impressions) as impressions
    , sum(ad_revenue) as ad_revenue
    , sum(delivered_ad_seconds) as ad_seconds
  from {{ ref('fct_bidder_impression_agg') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
  {{ dbt_utils.group_by(n=11) }}
)

, tbl_timezones as (
  select 'US/Pacific' as timezone
  union all
  select 'US/Eastern'
  union all
  select 'US/Mountain'
  union all 
  select 'US/Central'
  union all 
  select 'UTC'
)

select
  tbl_timezones.timezone as report_timezone
  , pre_agg.is_paid 
  , pre_agg.is_count
  , pre_agg.sov
  , pre_agg.bidder_name
  , pre_agg.network
  , pre_agg.adomain
  , pre_agg.bid_density
  , pre_agg.demand_partner
  , pre_agg.channel
  , pre_agg.sov__tier
  , pre_agg.impressions
  , pre_agg.ad_revenue
  , pre_agg.ad_seconds
  , convert_timezone(tbl_timezones.timezone, pre_agg.utc_ts) as report_time
from pre_agg
cross join tbl_timezones
order by report_timezone, report_time -- noqa: L054



