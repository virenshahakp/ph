{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set end_date_alias = dates.end_date_alias %}

{{ config(
materialized='table'
, alias='tbl_missing_vast_records_' + end_date_alias
, sort=['partition_date', 'partition_date_hour', 'player_pod_id' ]
, dist='player_pod_id'
, tags=["dai", "exclude_hourly", "exclude_daily"]
) }}

select
  partition_date
  , partition_date_hour
  , player_pod_id
  , creative_id
  , ad_system
  , ad_duration
  , impression_count
  , complete_count
  , first_beacon_logged_at
from {{ ref('tbl_ad_beacons_flat') }} as tbl_ad_beacons_flat
where not exists (
    select 1 as exists_check from {{ ref('tbl_vast_ads') }}
    where tbl_vast_ads.player_pod_id = tbl_ad_beacons_flat.player_pod_id
      and tbl_vast_ads.creative_id = tbl_ad_beacons_flat.creative_id
      and tbl_vast_ads.ad_system = tbl_ad_beacons_flat.ad_system
      and tbl_vast_ads.is_inserted is true
      and tbl_vast_ads.partition_date between ('{{ start_date }}'::date - interval '1 day')
        and ('{{ end_date }}'::date + interval '1 day') --noqa: L003
  )
  and partition_date between '{{ start_date }}' and '{{ end_date }}'