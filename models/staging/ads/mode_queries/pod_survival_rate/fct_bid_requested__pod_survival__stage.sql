{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
    , 'platform']
  , dist='even'
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , enabled=false
  , on_schema_change = 'append_new_columns'
 ) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with bid_requested as (
  select *
  from {{ ref('publica_bid_requested_dyn_source') }}
  where publica_bid_requested_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, publica_platform_map as (
  select *
  from {{ ref('dim_publica_platform_map') }}
)

, discriminate as (
  select
    bid_requested.partition_date
    , bid_requested.endpoint_uuid          as endpoint_uuid
    , bid_requested.requested_pod_duration as pod_length
    , 'publica'::varchar                   as ad_server
    , publica_platform_map.client_type     as platform
    , case
      when bid_requested.livestream = 1 then 'live'
      when bid_requested.livestream = 2 then 'vod'
      when bid_requested.livestream = 3 then 'dvr'
    end                                    as asset_type
    , lower(bid_requested.content_network) as network
    , lower(bid_requested.content_channel) as channel
    , bid_requested.adbreak_id
    || ':'
    || bid_requested.session_id            as pod_count
  from bid_requested
  left join publica_platform_map
    on bid_requested.bidrequest_site_id = publica_platform_map.bidrequest_site_id
)

select
  discriminate.partition_date
  , discriminate.platform
  , discriminate.asset_type
  , discriminate.network
  , discriminate.channel
  , discriminate.ad_server
  , hll(endpoint_uuid)          as endpoint_count
  , hll(discriminate.pod_count) as pod_count  --approx distinct for performance
from discriminate
{{ dbt_utils.group_by(n=6) }}


