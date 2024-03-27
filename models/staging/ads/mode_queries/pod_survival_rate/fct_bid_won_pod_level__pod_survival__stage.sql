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

with bid_won as (
  select *
  from {{ ref('publica_bid_won_dyn_source') }}
  where publica_bid_won_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select *
  from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select *
  from {{ ref('dim_publica_platform_map') }}
)

, ads as (
  select                                                                                  --noqa: L034
    bid_won.partition_date
    , publica_platform_map.client_type as platform
    , case
      when bid_won.livestream = 1 then 'live'
      when bid_won.livestream = 2 then 'vod'
      when bid_won.livestream = 3 then 'dvr'
    end                                as asset_type
    , lower(bid_won.content_network)   as network
    , lower(bid_won.content_channel)   as channel
    , bid_won.adbreak_id
    || ':'
    || bid_won.session_id              as pod_instance_id
    --, endpoint_uuid AS pods           --winning bids can be inserted into any endpoint in the vmap
    , 'publica'::varchar               as ad_server
  from bid_won
  left join demand_partner_map
    on bid_won.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
  left join publica_platform_map
    on bid_won.bidrequest_site_id = publica_platform_map.bidrequest_site_id
  {{ dbt_utils.group_by(n=7) }}
)

select
  partition_date
  , platform
  , asset_type
  , network
  , channel
  , ad_server
  , count(1) as bid_won_pod_count
from ads
{{ dbt_utils.group_by(n=6) }}
order by 1, 2, 3, 4, 5, 6