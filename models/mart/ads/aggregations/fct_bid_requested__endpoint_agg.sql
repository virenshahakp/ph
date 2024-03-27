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
    , 'vmap_uuid'
    , 'endpoint_uuid'
  ]
  , dist='endpoint_uuid'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , enabled=false
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with bid_requested as (
  select * from {{ ref('publica_bid_requested_dyn_source') }}
  where publica_bid_requested_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select * from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select * from {{ ref('dim_publica_platform_map') }}
)



select
  bid_requested.partition_date
  , bid_requested.partition_hour
  , publica_platform_map.client_type                  as platform
  , demand_partner_map.is_paid                        as is_paid
  , demand_partner_map.is_count                       as is_count
  , bid_requested.requested_pod_duration              as requested_pod_duration
  , bid_requested.bidrequest_device_geo_country       as geo_device_country
  , bid_requested.bidrequest_device_geo_region        as geo_device_region
  , bid_requested.bidrequest_device_geo_metro         as geo_device_metro
  , bid_requested.bidrequest_device_geo_city          as geo_device_city
  , bid_requested.bidrequest_device_geo_zip           as geo_device_zip
  , bid_requested.vmap_uuid                           as vmap_uuid
  , bid_requested.endpoint_uuid                       as endpoint_uuid
  , 'publica'::varchar                                as ad_server
  , lower(bid_requested.content_network)              as network
  , lower(bid_requested.content_channel)              as channel
  , case
    when bid_requested.livestream = 1 then 'live'
    when bid_requested.livestream = 2 then 'vod'
    when bid_requested.livestream = 3 then 'dvr'
  end                                                 as asset_type
  , count(1)                                          as request_count
  , count(distinct demand_partner_map.demand_partner) as demand_partner_count
  , count(distinct demand_partner_map.bidder_name)    as bidder_count
  , count(distinct bid_requested.bidder_tier)         as bidder_tier_count
from bid_requested
left join demand_partner_map
  on bid_requested.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bid_requested.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=17) }}