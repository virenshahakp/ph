{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
    , 'partition_hour'
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

with no_bid as (
  select * from {{ ref('publica_no_bid_dyn_source') }}
  where publica_no_bid_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select * from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select * from {{ ref('dim_publica_platform_map') }}
)


select
  no_bid.partition_date::date
  , no_bid.partition_hour::int
  , publica_platform_map.client_type                  as platform
  , demand_partner_map.is_paid                        as is_paid
  , demand_partner_map.is_count                       as is_count
  , no_bid.requested_pod_duration                     as requested_pod_duration
  , no_bid.bidrequest_device_geo_country              as geo_device_country
  , no_bid.bidrequest_device_geo_region               as geo_device_region
  , no_bid.bidrequest_device_geo_metro                as geo_device_metro
  , no_bid.bidrequest_device_geo_city                 as geo_device_city
  , no_bid.bidrequest_device_geo_zip                  as geo_device_zip
  , no_bid.vmap_uuid                                  as vmap_uuid
  , no_bid.endpoint_uuid                              as endpoint_uuid
  , 'publica'::varchar                                as ad_server 
  , lower(no_bid.content_network)                     as network
  , lower(no_bid.content_channel)                     as channel
  , case
    when no_bid.livestream = 1 then 'live'
    when no_bid.livestream = 2 then 'vod'
    when no_bid.livestream = 3 then 'dvr'
  end                                                 as asset_type
  , count(1)                                          as no_bid_count
  , count(distinct demand_partner_map.demand_partner) as demand_partner_count
  , count(distinct demand_partner_map.bidder_name)    as bidder_count
  , count(distinct no_bid.bidder_tier)                as bidder_tier_count
from no_bid
left join demand_partner_map
  on no_bid.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on no_bid.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=17) }}