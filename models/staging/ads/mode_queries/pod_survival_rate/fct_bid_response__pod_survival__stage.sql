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
  , dist='even'
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
 ) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with bid_response as (
  select *
  from {{ ref('publica_bid_response_dyn_source') }}
  where publica_bid_response_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, publica_platform_map as (
  select *
  from {{ ref('dim_publica_platform_map') }}
)

, mappings as (
  select
    bid_response.partition_date
    , bid_response.endpoint_uuid          as endpoint_uuid
    , bid_response.requested_pod_duration as pod_length
    , 'publica'::varchar                  as ad_server
    , publica_platform_map.client_type    as platform
    , case
      when bid_response.livestream = 1 then 'live'
      when bid_response.livestream = 2 then 'vod'
      when bid_response.livestream = 3 then 'dvr'
    end                                   as asset_type
    , lower(bid_response.content_network) as network
    , lower(bid_response.content_channel) as channel
    , bid_response.adbreak_id
    || ':'
    || bid_response.session_id            as pod_count
  from bid_response
  left join publica_platform_map
    on bid_response.bidrequest_site_id = publica_platform_map.bidrequest_site_id
)

select
  mappings.partition_date
  , mappings.platform
  , mappings.asset_type
  , mappings.network
  , mappings.channel
  , mappings.ad_server
  , hll(endpoint_uuid)        as endpoint_count
  , count(distinct pod_count) as pod_count
from mappings
{{ dbt_utils.group_by(6) }}