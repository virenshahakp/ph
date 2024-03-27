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
  , on_schema_change = 'append_new_columns'
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

select                                                                                              --noqa: L034
  date_add(
    'hour'
    , bidder_impression.partition_hour::int
    , bidder_impression.partition_date::date
  )                                                                     as partition_date_hour
  , bidder_impression.session_id || ':' || bidder_impression.adbreak_id as pod_instance_id
  , 'publica'::varchar                                                  as ad_server
  , publica_platform_map.client_type                                    as platform
  , lower(bidder_impression.content_network)                            as network
  , lower(bidder_impression.content_channel)                            as channel
  , case
    when bidder_impression.livestream = 1 then 'live'
    when bidder_impression.livestream = 2 then 'vod'
    when bidder_impression.livestream = 3 then 'dvr'
  end                                                                   as asset_type
  , demand_partner_map.pod_owner                                        as owner                   --noqa: L029
  -- ,case 
  --   when bidder_impression.philo_fallback = 1 then 'provider_primary'
  --   when bidder_impression.philo_fallback = 2 then 'provider_fallback'
  --   when bidder_impression.philo_fallback = 3 then 'philo_primary'
  --   when bidder_impression.philo_fallback = 4 then 'philo_backfill'
  --   else NULL
  -- end                                                                 as sov_split
  , bidder_impression.requested_pod_duration                            as requested_pod_duration
  , sum(
    case
      when demand_partner_map.is_paid = 'yes'
        then bidder_impression.delivered_pod_duration
      else 0::float
    end
  )                                                                     as paid_impression_seconds
  , sum(
    case
      when demand_partner_map.is_paid = 'no'
        then bidder_impression.delivered_pod_duration
      else 0::float
    end
  )                                                                     as unpaid_impression_seconds
  , sum(
    case
      when demand_partner_map.is_paid = 'yes'
        then 1
      else 0
    end
  )                                                                     as paid_impression_count
  , sum(
    case
      when demand_partner_map.is_paid = 'no'
        then 1
      else 0
    end
  )                                                                     as unpaid_impression_count
  , sum(
    case
      when demand_partner_map.is_paid = 'yes'
        then bidder_impression.bidresponse_cpm / 1000
      else 0::float
    end
  )                                                                     as paid_ad_revenue
  , sum(
    case
      when demand_partner_map.is_paid = 'no'
        then bidder_impression.bidresponse_cpm / 1000
      else 0::float
    end
  )                                                                     as unpaid_ad_revenue
from bidder_impression
left join demand_partner_map
  on bidder_impression.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bidder_impression.bidrequest_site_id = publica_platform_map.bidrequest_site_id
where demand_partner_map.is_count = 'yes'
{{ dbt_utils.group_by(n=9) }}
