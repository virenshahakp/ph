{{ config(
    materialized='tuple_incremental'
    , sort=['partition_date', 'player_pod_id']
    , dist='player_pod_id'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(1) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


select
  partition_date
  , player_pod_id -- noqa: disable=L016
  , sum(case when pod_owner = 'distributor' and is_house = 'House Ad' and beacon_type = 'impression' then 1 else 0 end)               as distributor_house_count_impression
  , sum(case when pod_owner = 'distributor' and is_house = 'House Ad' and beacon_type = 'complete' then 1 else 0 end)                 as distributor_house_count_complete
  , sum(case when pod_owner = 'distributor' and is_house = 'House Ad' and beacon_type = 'impression' then ad_duration else 0 end)     as distributor_house_duration_impression
  , sum(case when pod_owner = 'distributor' and is_house = 'House Ad' and beacon_type = 'complete' then ad_duration else 0 end)       as distributor_house_duration_complete

  , sum(case when pod_owner = 'distributor' and is_house = 'Not House Ad' and beacon_type = 'impression' then 1 else 0 end)           as distributor_nhouse_count_impression --need a better name for "not house"
  , sum(case when pod_owner = 'distributor' and is_house = 'Not House Ad' and beacon_type = 'complete' then 1 else 0 end)             as distributor_nhouse_count_complete
  , sum(case when pod_owner = 'distributor' and is_house = 'Not House Ad' and beacon_type = 'impression' then ad_duration else 0 end) as distributor_nhouse_duration_impression
  , sum(case when pod_owner = 'distributor' and is_house = 'Not House Ad' and beacon_type = 'complete' then ad_duration else 0 end)   as distributor_nhouse_duration_complete

  , sum(case when pod_owner = 'distributor' and beacon_type = 'impression' then 1 else 0 end)                                         as distributor_total_count_impression
  , sum(case when pod_owner = 'distributor' and beacon_type = 'complete' then 1 else 0 end)                                           as distributor_total_count_complete
  , sum(case when pod_owner = 'distributor' and beacon_type = 'impression' then ad_duration else 0 end)                               as distributor_total_duration_impression
  , sum(case when pod_owner = 'distributor' and beacon_type = 'complete' then ad_duration else 0 end)                                 as distributor_total_duration_complete

  , sum(case when pod_owner = 'provider' and is_house = 'House Ad' and beacon_type = 'impression' then 1 else 0 end)                  as provider_house_count_impression
  , sum(case when pod_owner = 'provider' and is_house = 'House Ad' and beacon_type = 'complete' then 1 else 0 end)                    as provider_house_count_complete
  , sum(case when pod_owner = 'provider' and is_house = 'House Ad' and beacon_type = 'impression' then ad_duration else 0 end)        as provider_house_duration_impression
  , sum(case when pod_owner = 'provider' and is_house = 'House Ad' and beacon_type = 'complete' then ad_duration else 0 end)          as provider_house_duration_complete

  , sum(case when pod_owner = 'provider' and is_house = 'Not House Ad' and beacon_type = 'impression' then 1 else 0 end)              as provider_nhouse_count_impression
  , sum(case when pod_owner = 'provider' and is_house = 'Not House Ad' and beacon_type = 'complete' then 1 else 0 end)                as provider_nhouse_count_complete
  , sum(case when pod_owner = 'provider' and is_house = 'Not House Ad' and beacon_type = 'impression' then ad_duration else 0 end)    as provider_nhouse_duration_impression
  , sum(case when pod_owner = 'provider' and is_house = 'Not House Ad' and beacon_type = 'complete' then ad_duration else 0 end)      as provider_nhouse_duration_complete

  , sum(case when pod_owner = 'provider' and beacon_type = 'impression' then 1 else 0 end)                                            as provider_total_count_impression
  , sum(case when pod_owner = 'provider' and beacon_type = 'complete' then 1 else 0 end)                                              as provider_total_count_complete
  , sum(case when pod_owner = 'provider' and beacon_type = 'impression' then ad_duration else 0 end)                                  as provider_total_duration_impression
  , sum(case when pod_owner = 'provider' and beacon_type = 'complete' then ad_duration else 0 end)                                    as provider_total_duration_complete
-- noqa: enable=L016
from {{ ref('tbl_ad_beacons') }}
where partition_date between '{{ start_date }}' and '{{ end_date }}'
group by 1, 2
