{{ config(
    materialized='tuple_incremental'
    , sort=['partition_date', 'player_pod_id']
    , dist='player_pod_id'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}



select
  (year || '-' || month || '-' || day)::date                                                                 as partition_date
  , player_id
  , client_type
  , channel                                                                                                  as channel_name
  , asset_type
  , ad_duration
  , received_at
  , pod_id
  --, ad_provider --always NULL, not populated
  --, ad_ident    --always NULL, not populated                  
  --, ad_reseller --always NULL or '', not populated
  , hour                                                                                                     as partition_hour
  , player_id                                                                                                as sutured_pid
  , pod_id || ':' || player_id                                                                               as player_pod_id
  , coalesce(provider_ident, ad_system || ':' || creative_id)                                                as provider_ident
  , coalesce(dup_ident, 'Deprecated in Stitcher')                                                            as dup_ident
  , coalesce(ad_system, split_part(provider_ident, ':', 1))                                                  as ad_system
  , coalesce(creative_id, split_part(provider_ident, ':', 2))                                                as creative_id
  , coalesce(
    pod_owner
    , case
      when owner = 0 then 'distributor'
      when owner = 1 then 'provider'
    end
  )                                                                                                          as pod_owner
  , case
    when is_house is true then 'House Ad'
    --stitcher does not correctly populate false values, instead it does not insert a value.  
    when is_house is null and pod_owner is not null then 'Not House Ad'
    else 'Unknown: Not provided in Sutured'
  end                                                                                                        as is_house
  , lower(beacon_type)                                                                                       as beacon_type
  , case when pod_owner is not null then 'stitcher' else 'sutured' end                                       as manifest_system
  , row_number() over (partition by player_pod_id, ad_system, creative_id, beacon_type order by received_at) as dedupe_number
  , date_add(
    'hour', partition_hour::int, partition_date::timestamp
  )                                                                                                          as partition_date_hour
from {{ source('spectrum_dai', 'beacons') }}
where partition_date between '{{ start_date }}' and '{{ end_date }}'


