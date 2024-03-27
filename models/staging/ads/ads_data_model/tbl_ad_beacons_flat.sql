{% set dates = get_update_dates(3) %} --this should have a larger lookback than other tables in the run
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set end_date_alias = dates.end_date_alias %}

{{ config(
    materialized='table'
    , alias='tbl_ad_beacons_flat_' + end_date_alias
    , sort=['partition_date', 'partition_date_hour', 'player_pod_id']
    , dist='player_pod_id'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_run_end_drop = true
) }}



--collapse impression and compelte becons into a single row.
--(mostly) only include information used to join to vast ads.

--aggregate impressions and complete beacons, assign to the first partition date for which they appear.  
with date_mapping as (
  select
    player_pod_id as player_pod_id
    , creative_id as creative_id
    , ad_system   as ad_system
    , ad_duration as ad_duration
    , first_value(partition_date) over (
      partition by player_pod_id, creative_id, ad_system
      order by partition_date, partition_date_hour, received_at rows between unbounded preceding and current row
    )             as partition_date
    , first_value(partition_date_hour) over (
      partition by player_pod_id, creative_id, ad_system
      order by partition_date, partition_date_hour, received_at rows between unbounded preceding and current row
    )             as partition_date_hour
    , first_value(received_at) over (
      partition by player_pod_id, creative_id, ad_system
      order by partition_date, partition_date_hour, received_at rows between unbounded preceding and current row
    )             as first_beacon_logged_at
    , case
      when beacon_type = 'impression' then 1 else 0
    end           as impression_counter
    , case
      when beacon_type = 'complete' then 1 else 0
    end           as complete_counter
  from {{ ref('tbl_ad_beacons') }}
  --allow for an additional day on each side
  where partition_date between '{{ start_date }}'::date - interval '1 day' and '{{ end_date }}'::date + interval '1 day'
)

select
  partition_date
  , partition_date_hour
  , player_pod_id
  , creative_id
  , ad_system
  , ad_duration
  , first_beacon_logged_at
  , sum(impression_counter) as impression_count
  , sum(complete_counter)   as complete_count
from date_mapping
where partition_date between '{{ start_date }}'::date and '{{ end_date }}'::date
{{ dbt_utils.group_by(n=7) }}


