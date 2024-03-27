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
  , on_schema_change = 'append_new_columns'
  , enabled = false
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with bidder_impression as (
  select * from {{ ref('publica_bidder_impression_dyn_source') }}
  where publica_bidder_impression_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select * from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select * from {{ ref('dim_publica_platform_map') }}
)

select
  bidder_impression.partition_date
  , bidder_impression.partition_hour
  , publica_platform_map.client_type                          as platform
  , demand_partner_map.is_paid                                as is_paid
  , demand_partner_map.is_count                               as is_count
  , bidder_impression.requested_pod_duration                  as requested_pod_duration
  , bidder_impression.bidrequest_device_geo_country           as geo_device_country
  , bidder_impression.bidrequest_device_geo_region            as geo_device_region
  , bidder_impression.bidrequest_device_geo_metro             as geo_device_metro
  , bidder_impression.bidrequest_device_geo_city              as geo_device_city
  , bidder_impression.bidrequest_device_geo_zip               as geo_device_zip
  , bidder_impression.vmap_uuid                               as vmap_uuid
  , bidder_impression.endpoint_uuid                           as endpoint_uuid
  , 'publica'::varchar                                        as ad_server
  , lower(bidder_impression.content_network)                  as network
  , lower(bidder_impression.content_channel)                  as channel
  , case
    when bidder_impression.livestream = 1 then 'live'
    when bidder_impression.livestream = 2 then 'vod'
    when bidder_impression.livestream = 3 then 'dvr'
  end                                                         as asset_type
  , sum(coalesce(bidder_impression.ad_duration, 0))           as ad_fill
  , count(1)                                                  as impression_count
  , count(distinct bidder_impression.ad_duration)             as distinct_ad_duration_count
  , count(distinct demand_partner_map.demand_partner)         as demand_partner_count
  , count(distinct demand_partner_map.bidder_name)            as bidder_count
  , count(distinct bidder_impression.bidder_tier)             as bidder_tier_count
  , count(distinct bidder_impression.custom_adomain)          as adomain_count
  , count(distinct bidder_impression.bidresponse_creative_id) as creative_id_count
  , count(distinct bidder_impression.bidresponse_cpm)         as distinct_cpm_count
  , min(bidder_impression.bidresponse_cpm)                    as min_cpm
  , max(bidder_impression.bidresponse_cpm)                    as max_cpm
  , sum(coalesce(bidder_impression.bidresponse_cpm, 0))       as cpm_sum  
from bidder_impression
left join demand_partner_map
  on bidder_impression.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bidder_impression.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=17) }}