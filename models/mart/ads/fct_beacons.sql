{{ config(
    materialized='tuple_incremental'
    , unique_key=['partition_date']
    , sort=['partition_date', 'sutured_pid']
    , dist='sutured_pid' 
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with

beacons as (

  select *
  from {{ ref('tbl_ad_beacons') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, user_id_lookup as (

  select
    user_id
    , sutured_pid
  from {{ ref('tbl_join_user_id__sutured_pid') }}

)

select
  user_id_lookup.user_id
  , beacons.client_type
  , beacons.channel_name
  , beacons.asset_type
  , beacons.provider_ident
  , beacons.dup_ident
  , beacons.ad_duration
  , beacons.received_at
  , beacons.pod_id
  , beacons.partition_date
  , beacons.partition_hour
  , null as is_error__multiple_events
  , beacons.ad_system
  , beacons.pod_owner
  , beacons.creative_id
  , beacons.beacon_type
  , beacons.is_house
  , beacons.manifest_system
  , beacons.sutured_pid
  , beacons.partition_date_hour
-- todo: , error_flagging.is_error__multiple_events__6000
-- todo: , error_flagging.is_error__multiple_events__15000
-- todo: , error_flagging.is_error__multiple_events__30000
from beacons
join user_id_lookup
  on beacons.sutured_pid = user_id_lookup.sutured_pid
