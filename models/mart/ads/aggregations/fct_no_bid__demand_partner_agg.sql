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
  no_bid.partition_date
  , no_bid.partition_hour
  , publica_platform_map.client_type                                     as platform
  , demand_partner_map.is_paid                                           as is_paid
  , demand_partner_map.is_count                                          as is_count
  , no_bid.requested_pod_duration                                        as requested_pod_duration
  , 'publica'::varchar                                                   as ad_server
  , lower(no_bid.content_network)                                        as network
  , lower(no_bid.content_channel)                                        as channel
  , case
    when no_bid.livestream = 1 then 'live'
    when no_bid.livestream = 2 then 'vod'
    when no_bid.livestream = 3 then 'dvr'
  end                                                                    as asset_type
  , lower(demand_partner_map.demand_partner)                             as demand_partner
  , count(distinct no_bid.adbreak_id || ':' || no_bid.session_id)        as pod_instance_id_count
  , count(distinct no_bid.endpoint_uuid)                                 as endpoint_uuid_count
  , count(distinct no_bid.vmap_uuid)                                     as vmap_uuid_count
  --winning bids can be inserted into any endpoint in the vmap
  , count(no_bid.vmap_uuid || ':' || no_bid.pod_number)                  as vmap_pod_id_count
  , sum(coalesce(no_bid.ad_duration, 0))                                 as ad_fill
  , count(1)                                                             as no_bid_count
  , count(distinct no_bid.ad_duration)                                   as distinct_ad_duration_count
  , count(distinct no_bid.bidder_tier)                                   as bidder_tier_count
  , sum(no_bid.number_of_ads_requested)                                  as number_of_ads_requested
from no_bid
left join demand_partner_map
  on no_bid.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on no_bid.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=11) }}