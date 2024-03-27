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
  , publica_platform_map.client_type                                          as platform
  , bid_response.requested_pod_duration                                       as pod_length
  , 'publica'::varchar                                                        as ad_server
  , case
    when bid_response.livestream = 1 then 'live'
    when bid_response.livestream = 2 then 'vod'
    when bid_response.livestream = 3 then 'dvr'
  end                                                                         as asset_type
  , lower(bid_response.content_network)                                       as network
  , lower(bid_response.content_channel)                                       as channel
  , lower(demand_partner_map.demand_partner)                                  as demand_partner
  , count(distinct bid_response.adbreak_id || ':' || bid_response.session_id) as pod_instance_id_count
  , count(distinct bid_response.endpoint_uuid)                                as endpoint_uuid_count
  , count(1)                                                                  as response_count
from bid_response
left join demand_partner_map
  on bid_response.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bid_response.bidrequest_site_id = publica_platform_map.bidrequest_site_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9