{{
  config(
    materialized='tuple_incremental'
    , sort=[
      'partition_date_hour'
      , 'channel'
      , 'network'
      , 'owner'
      , 'asset_type'
      , 'is_live_edge'
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

with pod_aggregate as (
  select
    player_pod_id
    , request_id
    , partition_date_hour
    , network
    , channel
    , owner
    , asset_type
    , is_live_edge
    , client_type
    , requested_pod_duration
    , pod_duration_group
    , distinct_inserted_ad_seconds
    , distinct_rendered_ads
    , distinct_rendered_ad_seconds
    , distinct_rendered_paid_ads
    , distinct_rendered_paid_ad_seconds
    , ad_revenue
    --, sov_adjusted_pod_duration_group: unnecessary, rationale:
    /*
      an sov transform on pod_duration_group would effect inserted and rendered
      pod equally at the granularity in the select clause and is therefore
      unnecessary in this calculation of the estimators
    */
    , is_inserted_pod
    , is_rendered_pod
  from {{ ref('tbl_vast_ads_to_pods_agg_view') }} as tbl_vast_ads_to_pods_agg_view
  where tbl_vast_ads_to_pods_agg_view.partition_date_hour between
    date_add('hour', 0::int, '{{ start_date }}')
    and date_add('hour', 23::int, '{{ end_date }}')
    {% if channel != "" %} and channel = '{{ channel }}' {% endif %}            --noqa: LT02
    and stitcher_status = 'ok' --rationale:
/*
        in order to determine what % of non-inserted, non-duplicate pods
        could have been seen by users, we first calculate what % of inserted
        pods (stitcher_status = 'ok') were seen (rendered) by users. Assuming
        that non-inserted pods have the same chance to be rendered as their
        inserted counterparts, we then apply the inserted pod render rate to
        the non-inserted pods to get the 'viewable but non-inserted' pod rate.
      */
)

--estimators: what are the inserted pod seconds vs the rendered pod sec
select
  partition_date_hour
  , network
  , channel
  , owner
  , asset_type
  , is_live_edge
  , client_type
  , pod_duration_group
  --estimator__pod_viewability
  , sum(
    case
      when is_inserted_pod = true
        then requested_pod_duration::int
      else 0
    end
  )                                                                             as inserted_pod_duration
  , sum(
    case
      when is_rendered_pod = true
        then requested_pod_duration::int
      else 0
    end
  )                                                                             as rendered_pod_duration
  , case
    when inserted_pod_duration > 0
      then rendered_pod_duration::float / inserted_pod_duration::float
    else 0
  end                                                                           as estimator__pod_viewability
  --estimator__ad_viewability
  , sum(
    case
      when is_rendered_pod = true
        then distinct_inserted_ad_seconds::int
      else 0
    end
  )                                                                             as inserted_ad_duration_rendered_pods
  , sum(
    case
      when is_rendered_pod = true
        then distinct_rendered_ad_seconds::int
      else 0
    end
  )                                                                             as rendered_ad_duration
  , case
    when inserted_ad_duration_rendered_pods > 0
      then rendered_ad_duration::float
        / inserted_ad_duration_rendered_pods::float
    else 0
  end                                                                           as estimator__ad_viewability
  , case 
    when sum(distinct_rendered_paid_ad_seconds::float) > 0 
      then (
        sum(ad_revenue::float)
        / sum(distinct_rendered_paid_ad_seconds::float)
      )
    else 0
  end                                                                           as opportunity__ave_rendered_paid_ad_revenue_per_second  --noqa: LT05
  , case
    when sum(distinct_rendered_paid_ads::float) > 0 
      then (
        sum(distinct_rendered_paid_ad_seconds::float)
        / sum(distinct_rendered_paid_ads::float)
      )
    else 0
  end                                                                           as opportunity__ave_rendered_paid_ad_length
  , case
    when sum(distinct_rendered_ads::float) > 0 
      then (
        sum(distinct_rendered_ad_seconds::float)
        / sum(distinct_rendered_ads::float)
      )
    else 0
  end                                                                           as opportunity__ave_rendered_ad_length
from pod_aggregate
{{ dbt_utils.group_by(n=8) }}
