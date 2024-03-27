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
    , unique_key=['partition_date_hour', 'channel']
    , on_schema_change='append_new_columns'
  )
}}

-- noqa: disable=LT01

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set channel = var("channel") %}

with pods as (
  select *
  from {{ ref('tbl_vast_ads_to_pods_agg_view') }} as tbl_vast_ads_to_pods_agg_view
  where tbl_vast_ads_to_pods_agg_view.partition_date_hour between
    date_add('hour', 0::int, '{{ start_date }}')
    and date_add('hour', 23::int, '{{ end_date }}')
    {% if channel != "" %} 
      and channel = '{{ channel }}' 
    {% else %}
      and channel is not null
    {% endif %}            --noqa: LT02
)

, opportunity as (
  select *
  from {{ ref('tbl_dim_opportunity_classifications_view') }}
  where tbl_dim_opportunity_classifications_view.partition_date_hour between
    date_add('hour', 0::int, '{{ start_date }}')
    and date_add('hour', 23::int, '{{ end_date }}')
    {% if channel != "" %} 
      and channel = '{{ channel }}' 
    {% else %}
      and channel is not null
    {% endif %}            --noqa: LT02
)

, tbl_ad_pods as (
  select * from {{ ref('tbl_ad_pods') }}
  where partition_date between '{{ start_date }}'::date + interval '1 day'
    and '{{ end_date }}'::date - interval '1 day'
)

--as columns are added to the underlying table, the ordering should be updated to maintain a logical grouping

select
  pods.user_id
  , {{ pod_instance_id(source = 'pods', player_id = 'sutured_pid') }} as pod_instance_id
  , pods.player_pod_id
  , pods.request_id
  , pods.sutured_pid
  , pods.pod_id
  , pods.partition_date_hour
  , pods.network
  , pods.channel
  , pods.owner
  , pods.asset_type
  , pods.is_live_edge
  , pods.client_type
  , pods.stitcher_status
  , pods.requested_pod_duration
  , pods.sov_adjusted_requested_pod_duration
  , pods.pod_duration_group
  , pods.sov_adjusted_pod_duration_group
  , pods.is_inserted_pod
  , pods.is_rendered_pod
  --inserted ads, ad seconds, ad revenue (distinct)
  , pods.distinct_inserted_ads
  , pods.distinct_inserted_ad_seconds
  , pods.inserted_ad_revenue
  --inserted paid ads, ad seconds, ad revenue (distinct)
  , pods.distinct_inserted_paid_ads
  , pods.distinct_inserted_paid_ad_seconds
  , pods.inserted_paid_ad_revenue
  --rendered ads, ad seconds, ad revenue (distinct and total)
  , pods.distinct_rendered_ads
  , pods.total_impressions
  , pods.distinct_rendered_ad_seconds
  , pods.total_rendered_ad_seconds
  --rendered paid ads
  , pods.distinct_rendered_paid_ads
  , pods.total_paid_impressions
  , pods.distinct_rendered_paid_ad_seconds
  , pods.total_rendered_paid_ad_seconds
  , pods.ad_revenue
  --completed ads, ad seconds (distinct and total)
  , pods.distinct_completed_ads
  , pods.total_completes
  , pods.distinct_completed_ad_seconds
  , pods.total_completed_ad_seconds
  --completed paid ads, ad seconds, ad revenue (distinct and total)
  , pods.distinct_completed_paid_ads
  , pods.total_paid_completes
  , pods.distinct_completed_paid_ad_seconds
  , pods.total_completed_paid_ad_seconds
  , pods.ad_revenue_completes
  , pods.has_uid_token
  , pods.has_aip_token
  --guaranteed unpaid ad (e.g. promos)
  , pods.distinct_inserted_guaranteed_unpaid_ads
  , pods.distinct_inserted_guaranteed_unpaid_ad_seconds
  , pods.distinct_rendered_guaranteed_unpaid_ads
  , pods.total_guaranteed_unpaid_impressions
  , pods.distinct_rendered_guaranteed_unpaid_ad_seconds
  , pods.total_rendered_guaranteed_unpaid_ad_seconds
  , pods.distinct_completed_guaranteed_unpaid_ads
  , pods.total_guaranteed_unpaid_completes
  , pods.distinct_completed_guaranteed_unpaid_ad_seconds
  , pods.total_completed_guaranteed_unpaid_ad_seconds
  --opportunity estimators and classifications
  , opportunity.estimator__pod_viewability
  , opportunity.estimator__ad_viewability
  , opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  , opportunity.opportunity__ave_rendered_paid_ad_length
  , opportunity.opportunity__ave_rendered_ad_length
  , tbl_ad_pods.event_timestamp as ad_pod_event_timestamp
  --monetizable_pod_seconds
  , (
    (
      case 
        when pods.is_inserted_pod = false
          then (
            pods.requested_pod_duration::int
          )
        else 0::int
      end
    )::float
    * coalesce(opportunity.estimator__pod_viewability, 0)::float
    * coalesce(opportunity.estimator__ad_viewability, 0)::float
  )                                                                             as monetizable_dropped_pod_seconds
  , (
    (
      case
        when pods.is_rendered_pod = true
          then 
            (
              pods.requested_pod_duration::int 
              - pods.distinct_inserted_ad_seconds::int
            )
        else 0::int
      end
    )::float
    * coalesce(opportunity.estimator__ad_viewability, 0)::float
  )                                                                             as monetizable_truncated_pod_seconds
  , (
    coalesce(pods.distinct_rendered_ad_seconds, 0)::float
    + coalesce(monetizable_dropped_pod_seconds, 0)::float
    + coalesce(monetizable_truncated_pod_seconds, 0)::float
  )                                                                             as monetizable_pod_seconds
  , case 
    when opportunity.opportunity__ave_rendered_paid_ad_length > 0
      then (
        monetizable_pod_seconds 
        / opportunity.opportunity__ave_rendered_paid_ad_length
      )
    else 0
  end                                                                           as monetizable_opportunities
  , (
    monetizable_dropped_pod_seconds
    * opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  )                                                                             as dropped_ad_sec_revenue_value
  , (
    monetizable_truncated_pod_seconds
    * opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  )                                                                             as truncated_ad_sec_revenue_value
  , (
    pods.ad_revenue
    + dropped_ad_sec_revenue_value
    + truncated_ad_sec_revenue_value
  )                                                                             as monetizable_space_revenue_value
  
  --monetizable_gauranteed_unpaid_adjusted_pod_seconds
  /*
    After testing is complete and this section is validated, its logic will be
    merged into the metrics above to abstract away the complexity of how
    monetizable space is calculated.
  */
  , (
    coalesce(pods.requested_pod_duration::int, 0)
    - coalesce(pods.distinct_inserted_guaranteed_unpaid_ad_seconds::int, 0)
  )                                                                             as requested_pod_duration__gauranteed_unpaid_removed
  , (
    coalesce(pods.distinct_inserted_ad_seconds::int, 0)
    - coalesce(pods.distinct_inserted_guaranteed_unpaid_ad_seconds::int, 0)
  )                                                                             as distinct_inserted_ad_seconds__gauranteed_unpaid_removed
  , (
    coalesce(pods.distinct_rendered_ad_seconds::int, 0)
    - coalesce(pods.distinct_rendered_guaranteed_unpaid_ad_seconds::int, 0)
  )                                                                             as distinct_rendered_ad_seconds__gauranteed_unpaid_removed
  , (
    (
      case 
        when pods.is_inserted_pod = false
          then (
            requested_pod_duration__gauranteed_unpaid_removed::int
          )
        else 0::int
      end
    )::float
    * coalesce(opportunity.estimator__pod_viewability, 0)::float
    * coalesce(opportunity.estimator__ad_viewability, 0)::float
  )                                                                             as monetizable_gauranteed_unpaid_removed_dropped_pod_seconds
  , (
    (
      case
        when pods.is_rendered_pod = true
          then 
            (
              requested_pod_duration__gauranteed_unpaid_removed::int 
              - distinct_inserted_ad_seconds__gauranteed_unpaid_removed::int
            )
        else 0::int
      end
    )::float
    * coalesce(opportunity.estimator__ad_viewability, 0)::float
  )                                                                             as monetizable_gauranteed_unpaid_removed_truncated_pod_seconds
  , (
    coalesce(distinct_rendered_ad_seconds__gauranteed_unpaid_removed, 0)::float
    + coalesce(monetizable_gauranteed_unpaid_removed_dropped_pod_seconds, 0)::float
    + coalesce(monetizable_gauranteed_unpaid_removed_truncated_pod_seconds, 0)::float
  )                                                                             as monetizable_gauranteed_unpaid_removed_pod_seconds
  , case
    when opportunity.opportunity__ave_rendered_paid_ad_length > 0 
      then (
        monetizable_gauranteed_unpaid_removed_pod_seconds 
        / opportunity.opportunity__ave_rendered_paid_ad_length
      )
    else 0
  end                                                                           as monetizable_opportunities__gauranteed_unpaid_removed
  , (
    monetizable_gauranteed_unpaid_removed_dropped_pod_seconds
    * opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  )                                                                             as dropped_ad_sec_revenue_value__gauranteed_unpaid_removed
  , (
    monetizable_gauranteed_unpaid_removed_truncated_pod_seconds
    * opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  )                                                                             as truncated_ad_sec_revenue_value__gauranteed_unpaid_removed
  , (
    pods.ad_revenue
    + dropped_ad_sec_revenue_value__gauranteed_unpaid_removed
    + truncated_ad_sec_revenue_value__gauranteed_unpaid_removed
  )                                                                             as monetizable_space_revenue_value__gauranteed_unpaid_removed
  
  --monetizable_sov_adjusted_pod_seconds
  , (
    (
      case 
        when pods.is_inserted_pod = false
          then pods.sov_adjusted_requested_pod_duration::int
        else 0::int
      end
    )::float
    * coalesce(opportunity.estimator__pod_viewability, 0)::float
    * coalesce(opportunity.estimator__ad_viewability, 0)::float
  )                                                                             as monetizable_sov_adjusted_dropped_pod_seconds
  , (
    (
      case 
        when pods.is_rendered_pod = true
          then
            (
              pods.sov_adjusted_requested_pod_duration::int 
              - pods.distinct_inserted_ad_seconds::int
            )
        else 0::int
      end
    )::float
    * coalesce(opportunity.estimator__ad_viewability, 0)::float
  )                                                                             as monetizable_sov_adjusted_truncated_pod_seconds
  , (
    coalesce(pods.distinct_rendered_ad_seconds, 0)::float
    + coalesce(monetizable_sov_adjusted_dropped_pod_seconds, 0)::float
    + coalesce(monetizable_sov_adjusted_truncated_pod_seconds, 0)::float
  )                                                                             as monetizable_sov_adjusted_pod_seconds
  , case 
    when opportunity.opportunity__ave_rendered_paid_ad_length > 0 
      then (
        monetizable_sov_adjusted_pod_seconds 
        / opportunity.opportunity__ave_rendered_paid_ad_length
      )
    else 0
  end                                                                           as monetizable_sov_adjusted_opportunities
  , (
    monetizable_sov_adjusted_dropped_pod_seconds
    * opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  )                                                                             as monetizable_sov_adjusted_dropped_ad_sec_revenue_value
  , (
    monetizable_sov_adjusted_truncated_pod_seconds
    * opportunity.opportunity__ave_rendered_paid_ad_revenue_per_second
  )                                                                             as monetizable_sov_adjusted_truncated_ad_sec_revenue_value
  , (
    pods.ad_revenue
    + monetizable_sov_adjusted_dropped_ad_sec_revenue_value
    + monetizable_sov_adjusted_truncated_ad_sec_revenue_value
  )                                                                             as monetizable_sov_adjusted_space_revenue_value

from pods
left join opportunity
  on pods.partition_date_hour = opportunity.partition_date_hour
    and pods.network = opportunity.network
    and pods.channel = opportunity.channel
    and pods.owner = opportunity.owner
    and pods.asset_type = opportunity.asset_type
    and pods.is_live_edge = opportunity.is_live_edge
    and pods.client_type = opportunity.client_type
    and pods.pod_duration_group = opportunity.pod_duration_group
left join tbl_ad_pods on pods.pod_instance_id = tbl_ad_pods.pod_instance_id    