{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date_hour']
  , sort=[
    'partition_date_hour'
    , 'asset_type'
    , 'network'
    , 'channel'
  ]
  , dist='channel'
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , enabled=false
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with bidder_impression as (
  select * from {{ ref('fct_bidder_impression__pod_instance_agg') }}
  where date_trunc('day', fct_bidder_impression__pod_instance_agg.partition_date_hour)
    between '{{ start_date }}'
    and '{{ end_date }}'
)

, bid_requested as (
  select * from {{ ref('fct_bid_requested__pod_instance_agg') }}
  where date_trunc('day', fct_bid_requested__pod_instance_agg.partition_date_hour)
    between '{{ start_date }}'
    and '{{ end_date }}'
)

, bidder_impressions_agg as (
  select
    partition_date_hour
    , channel
    , network
    , asset_type
    , sum(paid_impression_count)     as paid_impression_sum
    , sum(paid_impression_seconds)   as paid_impression_ad_seconds_sum
    , sum(unpaid_impression_count)   as unpaid_impression_sum
    , sum(unpaid_impression_seconds) as unpaid_impression_ad_seconds_sum
    , sum(paid_ad_revenue)           as paid_ad_revenue_sum
    , sum(unpaid_ad_revenue)         as unpaid_ad_revenue_sum
  from
    bidder_impression
  {{ dbt_utils.group_by(n=4) }}

)

, bid_requested_agg as (
  select
    partition_date_hour
    , channel
    , network
    , asset_type
    , sum(requested_pod_duration) as requested_ad_seconds
  from
    bid_requested
  {{ dbt_utils.group_by(n=4) }}

)

-- , bid_won_agg as (
--     SELECT
--       partition_date_hour
--       , network
--       , channel
--       , asset_type
--       , is_paid
--       , sum(opportunity)                    as opportunity_sum
--     From(
--      select
--         date_add(
--         'hour'
--         , bidder_impression.partition_hour::int
--         , bidder_impression.partition_date::date
--         )as partition_date_hour
--         , content_channel                   as channel
--         , content_network                   as network
--         , case 
--             when livestream=1 then 'live'
--             when livestream=2 then 'vod'
--             when livestream=3 then 'dvr'
--             end 
--                                             as "asset_type"  
--         , CASE
--         when bidresponse_cpm > 1 then 'yes'
--         else 'no'
--         end                                 as is_paid
--         , count(1)                          as opportunity
--         FROM publica.bid_won
--         where
--         date_add('hour', partition_hour::int, partition_date::date) 
--         between 
--             date_add('hour', 0, '{{ start_date }}')
--             and date_add('hour', 23, '{{ end_date }}')
--         group by 1,2,3,4,5
--         order by 1,2,3,4,5
--         )
--     GROUP by 1,2,3,4,5
-- )   

select
  bidder_impressions_agg.partition_date_hour
  , bidder_impressions_agg.network
  , bidder_impressions_agg.channel
  , bidder_impressions_agg.asset_type
  , bidder_impressions_agg.paid_impression_sum
  , bidder_impressions_agg.paid_impression_ad_seconds_sum
  , bidder_impressions_agg.unpaid_impression_sum
  , bidder_impressions_agg.unpaid_impression_ad_seconds_sum
  , bidder_impressions_agg.paid_ad_revenue_sum as gross_revenue_sum
  , bid_requested_agg.requested_ad_seconds
from bid_requested_agg
left join bidder_impressions_agg
  on bid_requested_agg.partition_date_hour = bidder_impressions_agg.partition_date_hour
    and bid_requested_agg.network = bidder_impressions_agg.network
    and bid_requested_agg.channel = bidder_impressions_agg.channel
    and bid_requested_agg.asset_type = bidder_impressions_agg.asset_type
