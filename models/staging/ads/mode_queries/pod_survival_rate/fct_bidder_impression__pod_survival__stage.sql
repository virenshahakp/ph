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
  ]
  , dist='platform'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with ads as (
  select
    publica_bidder_impression_dyn_source.partition_date
    , dim_publica_platform_map.client_type                                                                      as platform
    , publica_bidder_impression_dyn_source.endpoint_uuid                                                        as pods
    , publica_bidder_impression_dyn_source.requested_pod_duration                                               as pod_length
    , 'publica'::varchar                                                                                        as ad_server
    , case
      when publica_bidder_impression_dyn_source.livestream = 1 then 'live'
      when publica_bidder_impression_dyn_source.livestream = 2 then 'vod'
      when publica_bidder_impression_dyn_source.livestream = 3 then 'dvr'
    end                                                                                                         as asset_type
    , lower(publica_bidder_impression_dyn_source.content_network)                                               as network
    , lower(publica_bidder_impression_dyn_source.content_channel)                                               as channel
    , publica_bidder_impression_dyn_source.adbreak_id || ':' || publica_bidder_impression_dyn_source.session_id as pod_count
    , sum(publica_bidder_impression_dyn_source.ad_duration)                                                     as ad_fill
  from {{ ref('publica_bidder_impression_dyn_source') }}
  left join {{ ref('dim_demand_partner_map') }}
    on publica_bidder_impression_dyn_source.bidrequest_bids_headerbidder_id = dim_demand_partner_map.bidder_id
  left join {{ ref('dim_publica_platform_map') }}
    on publica_bidder_impression_dyn_source.bidrequest_site_id = dim_publica_platform_map.bidrequest_site_id
  where publica_bidder_impression_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select
  partition_date
  , platform
  , asset_type
  , network
  , channel
  , ad_server
  , count(1)                  as endpoint_count
  , sum(ad_fill)              as ad_fill
  , sum(pod_length)           as pod_length
  , count(distinct pod_count) as pod_count
from ads
group by 1, 2, 3, 4, 5, 6
order by 1, 2, 3, 4, 5, 6
