{{ 
  config(
    materialized='tuple_incremental'
    , sort=[
      'partition_date_hour'
      , 'channel'
      , 'network'
      , 'asset_type'
      , 'client_type'
      , 'pod_duration_group'
    ]
    , dist='channel'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , unique_key = ['partition_date_hour', 'channel']
    , on_schema_change = 'append_new_columns'
  )
}}

-- noqa: disable=LT01

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set channel = var("channel") %}

with bid_won as (

  select *
  from {{ ref('publica_bid_won_dyn_source') }}
  where
    ts between
    date_add('hour', 0::int, '{{ start_date }}') -- captures 00:00
    and date_add('day', 1::int, '{{ end_date }}') -- captures 23:59
    {% if channel != "" %} and content_channel = '{{ channel }}' {% endif %}    --noqa: LT02
)

, publica_platform_map as (

  select * from {{ ref('dim_publica_platform_map') }}

)

, pod_aggregate as (

  select
    bid_won.custom__cb                                                          as request_id
    , bid_won.adbreak_id                                                        as pod_id
    , date_trunc('hour', bid_won.ts)                                            as partition_date_hour
    , lower(bid_won.content_network)                                            as network
    , lower(bid_won.content_channel)                                            as channel
    , case
      when bid_won.livestream = 1 then 'live'
      when bid_won.livestream = 2 then 'vod'
      when bid_won.livestream = 3 then 'dvr'
    end                                                                         as asset_type
    --, is_live_edge unknown to publica
    , publica_platform_map.client_type                                          as client_type
    , case
      when bid_won.requested_pod_duration::int <= 60 then 'a: 0 - 60'
      when bid_won.requested_pod_duration::int <= 120 then 'b: 60 - 120'
      when bid_won.requested_pod_duration::int <= 180 then 'c: 120 - 180'
      when bid_won.requested_pod_duration::int <= 240 then 'd: 180 - 240'
      when bid_won.requested_pod_duration::int <= 300 then 'e: 240 - 300'
      when bid_won.requested_pod_duration::int <= 360 then 'f: 300 - 360'
      when bid_won.requested_pod_duration::int > 360 then 'g: 360+'
      else 'z: error in definition'
    end                                                                         as pod_duration_group
    , bid_won.requested_pod_duration::int                                       as requested_pod_duration
    , coalesce(
      sum(
        case
          when bid_won.philo_fallback in (1)
            then bid_won.ad_duration::int
        end
      ), 0
    )                                                                           as provider_primary_fill_duration
    , coalesce(
      sum(
        case
          when bid_won.philo_fallback in (2)
            then bid_won.ad_duration::int
        end
      ), 0
    )                                                                           as provider_backfill_fill_duration
    , coalesce(
      sum(
        case
          when bid_won.philo_fallback in (3)
            or bid_won.philo_fallback is null
            then bid_won.ad_duration::int
        end
      ), 0
    )                                                                           as philo_primary_fill_duration
    , coalesce(
      sum(
        case
          when bid_won.philo_fallback in (4)
            then bid_won.ad_duration::int
        end
      ), 0
    )                                                                           as philo_backfill_fill_duration
    , coalesce(sum(bid_won.ad_duration::int), 0)                                as fill_duration
  from bid_won
  left join publica_platform_map
    on bid_won.bidrequest_site_id = publica_platform_map.bidrequest_site_id
  {{ dbt_utils.group_by(n=9) }}

)

, fill_calculations as (

  select
    *
    , provider_primary_fill_duration + provider_backfill_fill_duration          as provider_fill_duration
    , philo_primary_fill_duration + philo_backfill_fill_duration                as philo_fill_duration
    , requested_pod_duration - fill_duration                                    as unfilled_duration
    , requested_pod_duration - provider_primary_fill_duration                   as requested_pod_duration__philo
  from pod_aggregate

)

--estimators
select
  partition_date_hour
  , network
  , channel
  , asset_type
  --, is_live_edge unknown to publica
  , client_type
  , pod_duration_group
  , sum(requested_pod_duration)                                                 as requested_pod_duration
  , sum(provider_fill_duration)                                                 as provider_fill_duration
  , sum(philo_fill_duration)                                                    as philo_fill_duration
  , sum(unfilled_duration)                                                      as unfilled_duration
  , sum(requested_pod_duration__philo)                                          as requested_pod_duration__philo
  , (
    sum(requested_pod_duration__philo::float)
    / sum(requested_pod_duration::float)                                        
  )                                                                             as estimator__pct_sov_share__philo
from fill_calculations
{{ dbt_utils.group_by(n=6) }}
