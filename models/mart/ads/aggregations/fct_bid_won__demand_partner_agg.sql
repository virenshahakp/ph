{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
    , 'platform'
    , 'asset_type'
    , 'network'
    , 'channel'
    , 'ad_server'
    , 'demand_partner'
  ]
  , dist='demand_partner'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with bid_won as (
  select * from {{ ref('publica_bid_won_dyn_source') }}
  where publica_bid_won_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select * from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select * from {{ ref('dim_publica_platform_map') }}
)


select
  bid_won.partition_date
  , bid_won.partition_hour
  , publica_platform_map.client_type                                as platform
  , demand_partner_map.is_paid                                      as is_paid
  , demand_partner_map.is_count                                     as is_count
  , bid_won.requested_pod_duration                                  as requested_pod_duration
  , 'publica'::varchar                                              as ad_server
  , lower(bid_won.content_network)                                  as network
  , lower(bid_won.content_channel)                                  as channel
  , case
    when bid_won.livestream = 1 then 'live'
    when bid_won.livestream = 2 then 'vod'
    when bid_won.livestream = 3 then 'dvr'
  end                                                               as asset_type
  , lower(demand_partner_map.demand_partner)                        as demand_partner
  , count(distinct bid_won.adbreak_id || ':' || bid_won.session_id) as pod_instance_id_count
  , count(distinct bid_won.endpoint_uuid)                           as endpoint_uuid_count
  , count(distinct bid_won.vmap_uuid)                               as vmap_uuid_count
  , count(distinct bid_won.vmap_uuid || ':' || bid_won.pod_number)  as vmap_pod_id_count
  , sum(coalesce(bid_won.ad_duration, 0))                           as ad_fill
  , count(1)                                                        as winning_ad_count
  , count(distinct bid_won.ad_duration)                             as distinct_ad_duration_count
  , count(distinct bid_won.bidder_tier)                             as bidder_tier_count
  , count(distinct bid_won.bid_density_bucket)                      as bid_density_bucket_count
  , count(distinct bid_won.custom_adomain)                          as adomain_count
  , count(distinct bid_won.bidresponse_creative_id)                 as creative_id_count
  , count(distinct bid_won.bidresponse_cpm)                         as distinct_cpm_count
  , min(bid_won.bidresponse_cpm)                                    as min_cpm
  , max(bid_won.bidresponse_cpm)                                    as max_cpm
  , sum(coalesce(bid_won.bidresponse_cpm, 0))                       as cpm_sum
from bid_won
left join demand_partner_map
  on bid_won.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bid_won.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=11) }}