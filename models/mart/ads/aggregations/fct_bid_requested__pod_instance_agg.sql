{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date_hour', 'channel']
  , sort=[
    'partition_date_hour'
    , 'pod_instance_id'
    , 'platform'
    , 'asset_type'
    , 'network'
    , 'channel'
    , 'owner'
    , 'ad_server' 
  ]
  , dist='pod_instance_id'
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

select                                                                                        --noqa: L034
  date_add(
    'hour'
    , bid_requested.partition_hour::int
    , bid_requested.partition_date::date
  )                                                             as partition_date_hour
  , bid_requested.session_id || ':' || bid_requested.adbreak_id as pod_instance_id
  , 'publica'::varchar                                          as ad_server
  , publica_platform_map.client_type                            as platform
  , lower(bid_requested.content_network)                        as network
  , lower(bid_requested.content_channel)                        as channel
  , case
    when bid_requested.livestream = 1 then 'live'
    when bid_requested.livestream = 2 then 'vod'
    when bid_requested.livestream = 3 then 'dvr'
  end                                                           as asset_type
  , demand_partner_map.pod_owner                                as owner                      --noqa: L029
  -- ,case 
  --   when bidder_impression.philo_fallback = 1 then 'provider_primary'
  --   when bidder_impression.philo_fallback = 2 then 'provider_fallback'
  --   when bidder_impression.philo_fallback = 3 then 'philo_primary'
  --   when bidder_impression.philo_fallback = 4 then 'philo_backfill'
  --   else NULL
  -- end                                                        as sov_split
  , bid_requested.requested_pod_duration                        as requested_pod_duration
from bid_requested
left join demand_partner_map
  on bid_requested.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bid_requested.bidrequest_site_id = publica_platform_map.bidrequest_site_id
where demand_partner_map.is_count = 'yes'
{{ dbt_utils.group_by(n=9) }}
