{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
    , 'ad_break_id']
  , dist='ad_break_id'
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

--noqa: disable=L034
select
  (year || '-' || month || '-' || day)::date                         as partition_date
  , ad_break_id
  , hour                                                             as partition_hour
  , asset_type
  , network
  , channel
  , callsign
  , ad_break_duration
  , is_dai_slot
  , dai_slot_owner
  , received_at                                                      as ad_break_received_at
  , created_at                                                       as ad_break_created_at
  , asset_id
  , created_at                                                       as ad_break_start_ts
  --this is an approximation.  The field was removed in https://gitlab.philo.com/product/sutured/-/merge_requests/595/
  , nullif(dai_slot_type, '')                                        as dai_slot_type
  , dateadd(ms, (ad_break_duration * 1000)::int, created_at)         as ad_break_end_ts --this is an approximation
  , version
  , regexp_count(callsign, '-TEST-', 1, 'i') > 0                     as is_test_channel
  , date_add('hour', partition_hour::int, partition_date::timestamp) as partition_date_hour
from {{ source('spectrum_dai', 'ad_breaks') }}
where (year || '-' || month || '-' || day)::date between '{{ start_date }}' and '{{ end_date }}'
order by partition_date, ad_break_id
