{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set end_date_alias = dates.end_date_alias %}

{{ config(
    materialized='table'
    , alias='tbl_orphaned_ad_pods_' + end_date_alias
    , sort=['partition_date']
    , dist='player_pod_id'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
) }}

select
  tbl_ad_pods.partition_date
  , tbl_ad_pods.partition_date_hour
  , tbl_ad_pods.pod_instance_id
  , tbl_ad_pods.player_pod_id
  , tbl_ad_pods.request_id
  , tbl_ad_pods.asset_type
  , tbl_ad_pods.client_type
  , tbl_ad_pods.network
  , tbl_ad_pods.channel
  , tbl_ad_pods.is_live_edge
  , tbl_ad_pods.owner
  , tbl_ad_pods.stitcher_status
  , tbl_ad_pods.has_uid_token
  , tbl_ad_pods.has_aip_token
  , tbl_ad_pods.duration
from {{ ref('tbl_ad_pods') }}
left outer join {{ ref('tbl_vast_ads') }}
  on tbl_vast_ads.pod_instance_id = tbl_ad_pods.pod_instance_id
    and tbl_vast_ads.partition_date between '{{ start_date }}'::date - interval '1 day'
    and '{{ end_date }}'::date + interval '1 day'
    and tbl_vast_ads.is_wrapper is false
left outer join {{ ref(('tbl_ad_beacons')) }}
  on tbl_ad_pods.player_pod_id = tbl_ad_beacons.player_pod_id
    and tbl_ad_beacons.partition_date between '{{ start_date }}'::date - interval '1 day'
    and '{{ end_date }}'::date + interval '1 day'
where tbl_vast_ads.pod_instance_id is null
  and tbl_ad_beacons.player_pod_id is null
  and tbl_ad_pods.partition_date between '{{ start_date }}' and '{{ end_date }}'
