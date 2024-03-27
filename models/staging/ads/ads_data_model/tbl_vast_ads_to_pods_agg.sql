{{
  config(
    materialized='tuple_incremental'
    , sort=[
      'partition_date_hour'
      , 'channel'
      , 'player_pod_id'
      , 'request_id'
    ]
    , dist='player_pod_id'
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

with vast_ads as (

  select
    *
    , replace(pod_duration_seconds, 's', '')::int                               as requested_pod_duration
    , case
      when requested_pod_duration::int <= 60 then 'a: 0 - 60'
      when requested_pod_duration::int <= 120 then 'b: 60 - 120'
      when requested_pod_duration::int <= 180 then 'c: 120 - 180'
      when requested_pod_duration::int <= 240 then 'd: 180 - 240'
      when requested_pod_duration::int <= 300 then 'e: 240 - 300'
      when requested_pod_duration::int <= 360 then 'f: 300 - 360'
      when requested_pod_duration::int > 360 then 'g: 360+'
      else 'z: error in definition'
    end                                                                         as pod_duration_group
  from {{ ref('tbl_vast_ads_enriched') }}
  where tbl_vast_ads_enriched.partition_date between
    '{{ start_date }}' and '{{ end_date }}'
    {% if channel != "" %} 
      and channel = '{{ channel }}' 
    {% else %}
      and channel is not null
    {% endif %}            --noqa: LT02
)

, sov_share as (

  select
    partition_date_hour
    , network
    , channel
    , asset_type
    --, is_live_edge unknown to publica
    , client_type
    , pod_duration_group
    , estimator__pct_sov_share__philo
  from {{ ref('tbl_sov_share_estimators') }}
  where
    tbl_sov_share_estimators.partition_date_hour between
    date_add('hour', 0::int, '{{ start_date }}')
    and date_add('hour', 23::int, '{{ end_date }}')
    {% if channel != "" %} 
      and channel = '{{ channel }}' 
    {% else %}
      and channel is not null
    {% endif %}            --noqa: LT02
)

select
  vast_ads.user_id
  , {{ pod_instance_id(source = 'vast_ads', player_id = 'sutured_pid') }} as pod_instance_id
  , vast_ads.player_pod_id
  , vast_ads.pod_id
  , vast_ads.request_id
  , vast_ads.sutured_pid
  , vast_ads.partition_date_hour
  , vast_ads.network
  , vast_ads.channel
  , vast_ads.owner
  , vast_ads.asset_type
  , vast_ads.is_live_edge
  , vast_ads.client_type
  , vast_ads.stitcher_status
  , vast_ads.requested_pod_duration
  , vast_ads.pod_duration_group
  , vast_ads.has_uid_token
  , vast_ads.has_aip_token
  , coalesce(vast_ads.stitcher_status = 'ok', false)                            as is_inserted_pod
  , (
    coalesce(vast_ads.requested_pod_duration, 0)::float
    * coalesce(sov_share.estimator__pct_sov_share__philo, 0)::float
  )                                                                             as sov_adjusted_requested_pod_duration
  , case
    when sov_adjusted_requested_pod_duration::int <= 60 then 'a: 0 - 60'
    when sov_adjusted_requested_pod_duration::int <= 120 then 'b: 60 - 120'
    when sov_adjusted_requested_pod_duration::int <= 180 then 'c: 120 - 180'
    when sov_adjusted_requested_pod_duration::int <= 240 then 'd: 180 - 240'
    when sov_adjusted_requested_pod_duration::int <= 300 then 'e: 240 - 300'
    when sov_adjusted_requested_pod_duration::int <= 360 then 'f: 300 - 360'
    when sov_adjusted_requested_pod_duration::int > 360 then 'g: 360+'
    else 'z: error in definition'
  end                                                                           as sov_adjusted_pod_duration_group
  , coalesce(sum(vast_ads.impression_count) >= 1, false)                        as is_rendered_pod
  --inserted ads, ad seconds, ad revenue (distinct)
  , sum(
    case
      when vast_ads.is_inserted is true
        then 1
      else 0
    end
  )                                                                             as distinct_inserted_ads
  , sum(
    case
      when vast_ads.is_inserted is true
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_inserted_ad_seconds
  , sum(
    case
      when vast_ads.is_inserted is true
        then coalesce({{ cpm_price() }}::float / 1000, 0)
      else 0
    end
  )                                                                             as inserted_ad_revenue
  --inserted paid ads, ad seconds, ad revenue (distinct)
  , sum(
    case
      when vast_ads.is_inserted is true
        and {{ cpm_price() }} > 1
        then 1
      else 0
    end
  )                                                                             as distinct_inserted_paid_ads
  , sum(
    case
      when vast_ads.is_inserted is true
        and {{ cpm_price() }} > 1
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_inserted_paid_ad_seconds
  , sum(
    case
      when vast_ads.is_inserted is true
        and {{ cpm_price() }} > 1
        then coalesce({{ cpm_price() }}::float / 1000, 0)
      else 0
    end
  )                                                                             as inserted_paid_ad_revenue
  --rendered ads, ad seconds, ad revenue (distinct and total)
  , sum(
    case
      when vast_ads.impression_count >= 1
        then 1
      else 0
    end
  )                                                                             as distinct_rendered_ads
  , sum(vast_ads.impression_count)                                              as total_impressions
  , sum(
    case
      when vast_ads.impression_count >= 1
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_rendered_ad_seconds
  , sum(
    case
      when vast_ads.impression_count >= 1
        then coalesce(vast_ads.duration * vast_ads.impression_count, 0)
      else 0
    end
  )                                                                             as total_rendered_ad_seconds

  -- rendered paid ads
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ cpm_price() }} > 1
        then 1
      else 0
    end
  )                                                                             as distinct_rendered_paid_ads
  , sum(
    case
      when {{ cpm_price() }} > 1
        then vast_ads.impression_count
      else 0
    end
  )                                                                             as total_paid_impressions
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ cpm_price() }} > 1
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_rendered_paid_ad_seconds
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ cpm_price() }} > 1
        then coalesce(vast_ads.duration * vast_ads.impression_count, 0)
      else 0
    end
  )                                                                             as total_rendered_paid_ad_seconds
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ cpm_price() }} > 1
        then coalesce({{ cpm_price() }}::float / 1000, 0)
      else 0
    end
  )                                                                             as ad_revenue

  --completed ads, ad seconds (distinct and total)
  , sum(
    case
      when vast_ads.complete_count >= 1
        then 1
      else 0
    end
  )                                                                             as distinct_completed_ads
  , sum(vast_ads.complete_count)                                                as total_completes
  , sum(
    case
      when vast_ads.complete_count >= 1
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_completed_ad_seconds
  , sum(
    case
      when vast_ads.complete_count >= 1
        then coalesce(vast_ads.duration * vast_ads.complete_count, 0)
      else 0
    end
  )                                                                             as total_completed_ad_seconds

  --completed paid ads, ad seconds, ad revenue (distinct and total)
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ cpm_price() }} > 1
        then 1
      else 0
    end
  )                                                                             as distinct_completed_paid_ads
  , sum(
    case
      when {{ cpm_price() }} > 1
        then vast_ads.complete_count
      else 0
    end
  )                                                                             as total_paid_completes
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ cpm_price() }} > 1
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_completed_paid_ad_seconds
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ cpm_price() }} > 1
        then coalesce(vast_ads.duration * vast_ads.complete_count, 0)
      else 0
    end
  )                                                                             as total_completed_paid_ad_seconds
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ cpm_price() }} > 1
        then coalesce({{ cpm_price() }}::float / 1000, 0)
      else 0
    end
  )                                                                             as ad_revenue_completes
  --TODO: [DEV-14517] unpaid obligated marketing, promo, philler ad seconds (inserted, rendered, completed)
  , sum(
    case
      when vast_ads.is_inserted is true
        and {{ is_guaranteed_unpaid() }} is true
        then 1
      else 0
    end
  )                                                                             as distinct_inserted_guaranteed_unpaid_ads
  , sum(
    case
      when vast_ads.is_inserted is true
        and {{ is_guaranteed_unpaid() }} is true
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_inserted_guaranteed_unpaid_ad_seconds
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ is_guaranteed_unpaid() }} is true
        then 1
      else 0
    end
  )                                                                             as distinct_rendered_guaranteed_unpaid_ads
  , sum(
    case
      when {{ is_guaranteed_unpaid() }} is true
        then vast_ads.impression_count
      else 0
    end
  )                                                                             as total_guaranteed_unpaid_impressions
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ is_guaranteed_unpaid() }} is true
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_rendered_guaranteed_unpaid_ad_seconds
  , sum(
    case
      when vast_ads.impression_count >= 1
        and {{ is_guaranteed_unpaid() }} is true
        then coalesce(vast_ads.duration * vast_ads.impression_count, 0)
      else 0
    end
  )                                                                             as total_rendered_guaranteed_unpaid_ad_seconds
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ is_guaranteed_unpaid() }} is true
        then 1
      else 0
    end
  )                                                                             as distinct_completed_guaranteed_unpaid_ads
  , sum(
    case
      when {{ is_guaranteed_unpaid() }} is true
        then vast_ads.complete_count
      else 0
    end
  )                                                                             as total_guaranteed_unpaid_completes
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ is_guaranteed_unpaid() }} is true
        then vast_ads.duration
      else 0
    end
  )                                                                             as distinct_completed_guaranteed_unpaid_ad_seconds
  , sum(
    case
      when vast_ads.complete_count >= 1
        and {{ is_guaranteed_unpaid() }} is true
        then coalesce(vast_ads.duration * vast_ads.complete_count, 0)
      else 0
    end
  )                                                                             as total_completed_guaranteed_unpaid_ad_seconds
  --TODO: [DEV-14518] viable ad seconds
from vast_ads
left join sov_share
  on vast_ads.partition_date_hour = sov_share.partition_date_hour
    and vast_ads.network = sov_share.network
    and vast_ads.channel = sov_share.channel
    and vast_ads.asset_type = sov_share.asset_type
    and vast_ads.client_type = sov_share.client_type
    and vast_ads.pod_duration_group = sov_share.pod_duration_group
{{ dbt_utils.group_by(n=21) }}

