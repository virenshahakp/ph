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
  , on_schema_change = "append_new_columns"
) }}

{% set end_date = var("end_date") %}
{% set start_date = var("start_date") %}
{% set default_lookback_days = 1 %}

{% if start_date == "" and end_date == "" %}

  {% set end_date = run_started_at.astimezone(modules.pytz.timezone("UTC")) %}
  {% set start_date = (end_date - modules.datetime.timedelta(default_lookback_days)) %}
  {% set end_date = end_date.date() %}
  {% set start_date = start_date.date() %}


{% endif %}

--v1.4.2

with fct_ad_pods__pod_survival__stage as (
  select
    partition_date
    , platform
    , asset_type
    , network
    , channel
    , ad_server
    , sum(pod_count)                                                        as step_1__ad_pods__pod_count
    , sum(requests_count)                                                   as step_2__ad_pods__requests_count
    , sum(ad_pods_status_ok_pod_count + ad_pods_status_underfill_pod_count) as step_6__ad_pods_status_ok__pod_count
    , sum(pod_duration)                                                     as step_1__ad_pods__requested_pod_duration
  from {{ ref('fct_ad_pods__pod_survival__stage') }}

  where partition_date between '{{ start_date }}' and '{{ end_date }}'

  group by 1, 2, 3, 4, 5, 6 --we don't technically need this aggergation, we're already at that level
)

-- , fct_bid_requested__pod_survival__stage AS (
--   SELECT
--     partition_date
--     , platform
--     , asset_type
--     , network
--     , channel
--     , ad_server
--     , SUM(endpoint_count) AS step_3__bid_requested__endpoint_count
--     , sum(pod_count) as step_3__bid_requested__pod_count
--   FROM dbt_staging.fct_bid_requested__pod_survival__stage

--   WHERE partition_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'

--   GROUP BY 1, 2, 3, 4, 5, 6
-- )

, fct_bid_response__pod_survival__stage as (
  select
    partition_date
    , platform
    , asset_type
    , network
    , channel
    , ad_server
    , sum(endpoint_count) as step_4__bid_response__endpoint_count
    , sum(pod_count)      as step_4__bid_response__pod_count
  from {{ ref('fct_bid_response__pod_survival__stage') }}

  where partition_date between '{{ start_date }}' and '{{ end_date }}'

  group by 1, 2, 3, 4, 5, 6
)

, fct_bid_won__pod_survival__stage as (
  select
    partition_date
    , platform
    , asset_type
    , network
    , channel
    , ad_server
    , sum(endpoint_count) as step_5__bid_won__vmap_pod_number_count

  from {{ ref('fct_bid_won__pod_survival__stage') }}

  where partition_date between '{{ start_date }}' and '{{ end_date }}'

  group by 1, 2, 3, 4, 5, 6
)

, fct_beacons__pod_survival__stage as (
  select
    partition_date
    , platform
    , asset_type
    , network
    , channel
    , ad_server
    , sum(pods_count) as step_7__beacons__pod_count

  from {{ ref('fct_beacons__pod_survival__stage') }}

  where partition_date between '{{ start_date }}' and '{{ end_date }}'

  group by 1, 2, 3, 4, 5, 6
)


, fct_bidder_impression__pod_survival__stage as (
  select
    partition_date
    , platform
    , asset_type
    , network
    , channel
    , ad_server
    , sum(endpoint_count) as step_8__bidder_impression__endpoint_count
    , sum(pod_count)      as step_8__bidder_impression__pod_count
  from {{ ref('fct_bidder_impression__pod_survival__stage') }}

  where partition_date between '{{ start_date }}' and '{{ end_date }}'

  group by 1, 2, 3, 4, 5, 6
)

, fct_bid_won_pod_level__pod_survival__stage as (
  select
    partition_date
    , platform
    , asset_type
    , network
    , channel
    , ad_server
    , sum(bid_won_pod_count) as step_5__bid_won__pod_count

  from {{ ref('fct_bid_won_pod_level__pod_survival__stage') }}

  where partition_date between '{{ start_date }}' and '{{ end_date }}'

  group by 1, 2, 3, 4, 5, 6
)


, waterfall_data as (
  select
    fct_ad_pods__pod_survival__stage.partition_date::timestamp as partition_date
    , fct_ad_pods__pod_survival__stage.platform
    , fct_ad_pods__pod_survival__stage.asset_type
    , fct_ad_pods__pod_survival__stage.network
    , fct_ad_pods__pod_survival__stage.channel
    , fct_ad_pods__pod_survival__stage.ad_server

    , fct_ad_pods__pod_survival__stage.step_1__ad_pods__pod_count

    , fct_ad_pods__pod_survival__stage.step_2__ad_pods__requests_count

    -- , fct_bid_requested__pod_survival__stage.step_3__bid_requested__endpoint_count
    -- , fct_bid_requested__pod_survival__stage.step_3__bid_requested__pod_count

    , fct_bid_response__pod_survival__stage.step_4__bid_response__endpoint_count
    , fct_bid_response__pod_survival__stage.step_4__bid_response__pod_count

    , fct_bid_won__pod_survival__stage.step_5__bid_won__vmap_pod_number_count
    , fct_bid_won_pod_level__pod_survival__stage.step_5__bid_won__pod_count
    --step_5a__vast_ads_pod_count

    , fct_ad_pods__pod_survival__stage.step_6__ad_pods_status_ok__pod_count

    , fct_beacons__pod_survival__stage.step_7__beacons__pod_count

    , fct_bidder_impression__pod_survival__stage.step_8__bidder_impression__endpoint_count

  from fct_ad_pods__pod_survival__stage

  -- LEFT JOIN fct_bid_requested__pod_survival__stage
  --   ON fct_ad_pods__pod_survival__stage.partition_date = fct_bid_requested__pod_survival__stage.partition_date
  --     AND fct_ad_pods__pod_survival__stage.network = fct_bid_requested__pod_survival__stage.network
  --     AND fct_ad_pods__pod_survival__stage.channel = fct_bid_requested__pod_survival__stage.channel
  --     AND fct_ad_pods__pod_survival__stage.platform = fct_bid_requested__pod_survival__stage.platform
  --     AND fct_ad_pods__pod_survival__stage.asset_type = fct_bid_requested__pod_survival__stage.asset_type 
  --     AND fct_ad_pods__pod_survival__stage.ad_server = fct_bid_requested__pod_survival__stage.ad_server 

  left join fct_bid_response__pod_survival__stage
    on fct_ad_pods__pod_survival__stage.partition_date = fct_bid_response__pod_survival__stage.partition_date
      and fct_ad_pods__pod_survival__stage.network = fct_bid_response__pod_survival__stage.network
      and fct_ad_pods__pod_survival__stage.channel = fct_bid_response__pod_survival__stage.channel
      and fct_ad_pods__pod_survival__stage.platform = fct_bid_response__pod_survival__stage.platform
      and fct_ad_pods__pod_survival__stage.asset_type = fct_bid_response__pod_survival__stage.asset_type
      and fct_ad_pods__pod_survival__stage.ad_server = fct_bid_response__pod_survival__stage.ad_server

  left join fct_bid_won__pod_survival__stage
    on fct_ad_pods__pod_survival__stage.partition_date = fct_bid_won__pod_survival__stage.partition_date
      and fct_ad_pods__pod_survival__stage.network = fct_bid_won__pod_survival__stage.network
      and fct_ad_pods__pod_survival__stage.channel = fct_bid_won__pod_survival__stage.channel
      and fct_ad_pods__pod_survival__stage.platform = fct_bid_won__pod_survival__stage.platform
      and fct_ad_pods__pod_survival__stage.asset_type = fct_bid_won__pod_survival__stage.asset_type
      and fct_ad_pods__pod_survival__stage.ad_server = fct_bid_won__pod_survival__stage.ad_server

  left join fct_beacons__pod_survival__stage
    on fct_ad_pods__pod_survival__stage.partition_date = fct_beacons__pod_survival__stage.partition_date
      and fct_ad_pods__pod_survival__stage.network = fct_beacons__pod_survival__stage.network
      and fct_ad_pods__pod_survival__stage.channel = fct_beacons__pod_survival__stage.channel
      and fct_ad_pods__pod_survival__stage.platform = fct_beacons__pod_survival__stage.platform
      and fct_ad_pods__pod_survival__stage.asset_type = fct_beacons__pod_survival__stage.asset_type
      and fct_ad_pods__pod_survival__stage.ad_server = fct_beacons__pod_survival__stage.ad_server

  left join fct_bidder_impression__pod_survival__stage
    on fct_ad_pods__pod_survival__stage.partition_date = fct_bidder_impression__pod_survival__stage.partition_date
      and fct_ad_pods__pod_survival__stage.network = fct_bidder_impression__pod_survival__stage.network
      and fct_ad_pods__pod_survival__stage.channel = fct_bidder_impression__pod_survival__stage.channel
      and fct_ad_pods__pod_survival__stage.platform = fct_bidder_impression__pod_survival__stage.platform
      and fct_ad_pods__pod_survival__stage.asset_type = fct_bidder_impression__pod_survival__stage.asset_type
      and fct_ad_pods__pod_survival__stage.ad_server = fct_bidder_impression__pod_survival__stage.ad_server

  left join fct_bid_won_pod_level__pod_survival__stage
    on fct_ad_pods__pod_survival__stage.partition_date = fct_bid_won_pod_level__pod_survival__stage.partition_date
      and fct_ad_pods__pod_survival__stage.network = fct_bid_won_pod_level__pod_survival__stage.network
      and fct_ad_pods__pod_survival__stage.channel = fct_bid_won_pod_level__pod_survival__stage.channel
      and fct_ad_pods__pod_survival__stage.platform = fct_bid_won_pod_level__pod_survival__stage.platform
      and fct_ad_pods__pod_survival__stage.asset_type = fct_bid_won_pod_level__pod_survival__stage.asset_type
      and fct_ad_pods__pod_survival__stage.ad_server = fct_bid_won_pod_level__pod_survival__stage.ad_server

)

, unpivoted_data as (
  select *
  from waterfall_data unpivot include nulls (
    metric_count for metric in (
      step_1__ad_pods__pod_count


      , step_2__ad_pods__requests_count

      -- , step_3__bid_requested__endpoint_count
      -- , step_3__bid_requested__pod_count

      , step_4__bid_response__endpoint_count
      , step_4__bid_response__pod_count

      , step_5__bid_won__vmap_pod_number_count
      , step_5__bid_won__pod_count

      , step_6__ad_pods_status_ok__pod_count

      , step_7__beacons__pod_count

      , step_8__bidder_impression__endpoint_count
    )
  )
  order by metric, partition_date --noqa: AM06
)

select
  *
  , 'daily'                     as aggregation_level
  , split_part(metric, '__', 1) as waterfall_step
  , split_part(metric, '__', 2) as datasource
  , split_part(metric, '__', 3) as metric_unit
  , coalesce(metric_count, 0) - coalesce(first_value(metric_count) over (
    partition by
      partition_date
      , platform
      , asset_type
      , network
      , channel
      , ad_server
    order by
      waterfall_step
      asc
    rows between unbounded preceding and unbounded following
  ), 0)                         as from_step1_difference
  , coalesce(first_value(metric_count) over (
    partition by
      partition_date
      , platform
      , asset_type
      , network
      , channel
      , ad_server
    order by
      waterfall_step
      asc
    rows between unbounded preceding and unbounded following
  ), 0)                         as from_step1_base
  , coalesce(metric_count, 0) - coalesce(lag(metric_count) over (
    partition by
      partition_date
      , platform
      , asset_type
      , network
      , channel
      , ad_server
    order by
      waterfall_step
  ), 0)                         as previous_step_difference
  , coalesce(lag(metric_count) over (
    partition by
      partition_date
      , platform
      , asset_type
      , network
      , channel
      , ad_server
    order by
      waterfall_step
  ), 0)                         as previous_step_base
from unpivoted_data