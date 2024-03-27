{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=['partition_date'
      , 'partition_hour']
  , dist='md5_hash'
  , full_refresh = false
  , tags=["exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
  , enabled = false
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with windowed_distincts as (
  select
    bid_response.partition_date::date 
    , bid_response.partition_hour
    , dim_demand_partner_map.demand_partner
    , bid_response.fill_type_classification
    , bid_response.pod_owner
    , bid_response.content_network as network
    , bid_response.content_channel as channel
    , date_add('hour', bid_response.partition_hour::int, bid_response.partition_date::timestamp) as utc_ts
    , right(bid_response.targeting_device, length(bid_response.targeting_device) - 23) as platform
    , md5(
      bid_response.partition_date
      || bid_response.partition_hour
      || bid_response.utc_ts
      || dim_demand_partner_map.demand_partner
      || bid_response.fill_type_classification
      || bid_response.pod_owner
      || bid_response.platform
      || bid_response.network
      || bid_response.channel
    ) as md5_hash

    , dense_rank() over (
      partition by
        bid_response.partition_date
        , bid_response.partition_hour
        , bid_response.utc_ts
        , dim_demand_partner_map.demand_partner
        , bid_response.fill_type_classification
        , bid_response.pod_owner
        , bid_response.platform
        , bid_response.network
        , bid_response.channel
      order by bid_response.endpoint_uuid
    ) as opportunity__dense_rank

    , dense_rank() over (
      partition by
        bid_response.partition_date
        , bid_response.partition_hour
        , bid_response.utc_ts
        , bid_response.fill_type_classification
        , bid_response.pod_owner
        , bid_response.platform
        , bid_response.network
        , bid_response.channel
      order by bid_response.endpoint_uuid
    ) as pod__dense_rank

  from {{ ref('publica_bid_response_dyn_source') }} 
  left join {{ ref('dim_demand_partner_map') }} 
    on bid_response.bidrequest_bids_headerbidder_id = dim_demand_partner_map.bidder_id
  where bid_response.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, bid_response as (
  select
    partition_date
    , partition_hour
    , utc_ts
    , demand_partner
    , fill_type_classification
    , pod_owner
    , platform
    , network
    , channel
    , md5_hash

    , count(1) as bid_response__count
  from windowed_distincts
  {{ dbt_utils.group_by(n=10) }}
)

, opportunity_and_pods as (
  select distinct  --eeek
    partition_date
    , partition_hour
    , utc_ts
    , demand_partner
    , fill_type_classification
    , pod_owner
    , platform
    , network
    , channel
    , md5_hash

    , max(opportunity__dense_rank) over (
      partition by 
        partition_date
        , partition_hour
        , utc_ts
        , demand_partner
        , fill_type_classification
        , pod_owner
        , platform
        , network
        , channel
    ) as opportunity__count

    , max(pod__dense_rank) over (
      partition by 
        partition_date
        , partition_hour
        , utc_ts
        , fill_type_classification
        , pod_owner
        , platform
        , network
        , channel
    ) as pod__count

  from windowed_distincts
)

select 
  opportunity_and_pods.partition_date
  , opportunity_and_pods.partition_hour
  , opportunity_and_pods.utc_ts
  , opportunity_and_pods.demand_partner
  , opportunity_and_pods.fill_type_classification
  , opportunity_and_pods.pod_owner
  , opportunity_and_pods.platform
  , opportunity_and_pods.network
  , opportunity_and_pods.channel
  , opportunity_and_pods.opportunity__count
  , opportunity_and_pods.pod__count
  , bid_response.bid_response__count
  , opportunity_and_pods.md5_hash
from opportunity_and_pods
join bid_response
  on opportunity_and_pods.md5_hash = bid_response.md5_hash
