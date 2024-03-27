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


with bid_response as (
  select * from {{ ref('publica_bid_response_dyn_source') }}
  where publica_bid_response_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select * from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select * from {{ ref('dim_publica_platform_map') }}
)

select
  bid_response.partition_date
  , bid_response.partition_hour
  , publica_platform_map.client_type                     as platform
  , demand_partner_map.is_paid                           as is_paid
  , demand_partner_map.is_count                          as is_count
  , bid_response.requested_pod_duration                  as requested_pod_duration
  , bid_response.bidrequest_device_geo_country           as geo_device_country
  , bid_response.bidrequest_device_geo_region            as geo_device_region
  , bid_response.bidrequest_device_geo_metro             as geo_device_metro
  , bid_response.bidrequest_device_geo_city              as geo_device_city
  , bid_response.bidrequest_device_geo_zip               as geo_device_zip
  , bid_response.vmap_uuid                               as vmap_uuid
  , bid_response.endpoint_uuid                           as endpoint_uuid
  , 'publica'::varchar                                   as ad_server
  , lower(bid_response.content_network)                  as network
  , lower(bid_response.content_channel)                  as channel
  , case
    when bid_response.livestream = 1 then 'live'
    when bid_response.livestream = 2 then 'vod'
    when bid_response.livestream = 3 then 'dvr'
  end                                                    as asset_type
  , count(1)                                             as response_count
  , count(distinct demand_partner_map.demand_partner)    as demand_partner_count
  , count(distinct demand_partner_map.bidder_name)       as bidder_count
  , count(distinct bid_response.bidder_tier)             as bidder_tier_count
  , count(distinct bid_response.bid_density_bucket)      as bid_density_bucket_count
  , count(distinct bid_response.bidresponse_creative_id) as creative_id_count
  , count(distinct bid_response.bidresponse_cpm)         as distinct_cpm_count
  , min(bid_response.bidresponse_cpm)                    as min_cpm
  , max(bid_response.bidresponse_cpm)                    as max_cpm
  , sum(coalesce(bid_response.bidresponse_cpm, 0))       as cpm_sum
from bid_response
left join demand_partner_map
  on bid_response.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bid_response.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=17) }}