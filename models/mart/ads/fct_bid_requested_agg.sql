{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
      'partition_date'
      , 'partition_hour'
      , 'md5_hash']
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
    publica_bid_requested_dyn_source.partition_date::date 
    , publica_bid_requested_dyn_source.partition_hour
    , dim_demand_partner_map.demand_partner
    , publica_bid_requested_dyn_source.fill_type_classification
    , publica_bid_requested_dyn_source.pod_owner
    , publica_bid_requested_dyn_source.content_network as network
    , publica_bid_requested_dyn_source.content_channel as channel
    , date_add('hour', publica_bid_requested_dyn_source.partition_hour::int, publica_bid_requested_dyn_source.partition_date::timestamp) as utc_ts
    , right(publica_bid_requested_dyn_source.targeting_device, length(publica_bid_requested_dyn_source.targeting_device) - 23) as platform
    , md5(
      publica_bid_requested_dyn_source.partition_date
      || publica_bid_requested_dyn_source.partition_hour
      || publica_bid_requested_dyn_source.utc_ts
      || dim_demand_partner_map.demand_partner
      || publica_bid_requested_dyn_source.fill_type_classification
      || publica_bid_requested_dyn_source.pod_owner
      || publica_bid_requested_dyn_source.platform
      || publica_bid_requested_dyn_source.network
      || publica_bid_requested_dyn_source.channel
    ) as md5_hash

    , dense_rank() over (
      partition by
        publica_bid_requested_dyn_source.partition_date
        , publica_bid_requested_dyn_source.partition_hour
        , publica_bid_requested_dyn_source.utc_ts
        , dim_demand_partner_map.demand_partner
        , publica_bid_requested_dyn_source.fill_type_classification
        , publica_bid_requested_dyn_source.pod_owner
        , publica_bid_requested_dyn_source.platform
        , publica_bid_requested_dyn_source.network
        , publica_bid_requested_dyn_source.channel
      order by publica_bid_requested_dyn_source.endpoint_uuid
    ) as opportunity__dense_rank

    , dense_rank() over (
      partition by
        publica_bid_requested_dyn_source.partition_date
        , publica_bid_requested_dyn_source.partition_hour
        , publica_bid_requested_dyn_source.utc_ts
        , publica_bid_requested_dyn_source.fill_type_classification
        , publica_bid_requested_dyn_source.pod_owner
        , publica_bid_requested_dyn_source.platform
        , publica_bid_requested_dyn_source.network
        , publica_bid_requested_dyn_source.channel
      order by publica_bid_requested_dyn_source.endpoint_uuid
    ) as pod__dense_rank

  from {{ ref('publica_bid_requested_dyn_source') }} 
  left join {{ ref('dim_demand_partner_map') }} 
    on publica_bid_requested_dyn_source.bidrequest_bids_headerbidder_id = dim_demand_partner_map.bidder_id

  where publica_bid_requested_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, bid_request as (
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
    , count(1) as bid_request__count
  from windowed_distincts
  {{ dbt_utils.group_by(n=10) }}
)

, opportunity_and_pods as (
  select distinct
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
  , date_add('hour', '')
  , opportunity_and_pods.utc_ts
  , opportunity_and_pods.demand_partner
  , opportunity_and_pods.fill_type_classification
  , opportunity_and_pods.pod_owner
  , opportunity_and_pods.platform
  , opportunity_and_pods.network
  , opportunity_and_pods.channel
  , opportunity_and_pods.opportunity__count
  , opportunity_and_pods.pod__count
  , bid_request.bid_request__count
  , opportunity_and_pods.md5_hash
from opportunity_and_pods
join bid_request
  on opportunity_and_pods.md5_hash = bid_request.md5_hash


