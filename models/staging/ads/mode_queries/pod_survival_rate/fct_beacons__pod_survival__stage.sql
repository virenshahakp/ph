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
  , dist='platform'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'  
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with ads as (
  select
    partition_date
    , client_type                  as platform
    , asset_type
    , channel_name                 as channel
    , case
      when lower(channel_name) = 'gac' then 'discovery'
      when lower(channel_name) = 'ci' then 'aetv'
      when lower(channel_name) = 'hallmark' then 'hallmark'
      when lower(channel_name) = 'sony' then 'sony'
      when lower(channel_name) = 'mtv' then 'viacom'
      when lower(channel_name) = 'ifc' then 'amc'
      when lower(channel_name) = 'insp' then 'insp'
      when lower(channel_name) = 'comedy' then 'viacom'
      when lower(channel_name) = 'nik' then 'viacom'
      when lower(channel_name) = 'bether' then 'viacom'
      when lower(channel_name) = 'discoveryfamily' then 'discovery'
      when lower(channel_name) = 'cinemoi' then 'mvm'
      when lower(channel_name) = 'kin' then 'kin'
      when lower(channel_name) = 'hmm' then 'hallmark'
      when lower(channel_name) = 'lawcrime' then 'lawcrime'
      when lower(channel_name) = 'sci' then 'discovery'
      when lower(channel_name) = 'vh1' then 'viacom'
      when lower(channel_name) = 'food' then 'scripps'
      when lower(channel_name) = 'discoverylife' then 'discovery'
      when lower(channel_name) = 'paramount' then 'viacom'
      when lower(channel_name) = 'mtv2' then 'viacom'
      when lower(channel_name) = 'gacliving' then 'discovery'
      when lower(channel_name) = 'motortrend' then 'discovery'
      when lower(channel_name) = 'people' then 'people'
      when lower(channel_name) = 'fyi' then 'aetv'
      when lower(channel_name) = 'hdnet' then 'axs'
      when lower(channel_name) = 'gsn' then 'gsn'
      when lower(channel_name) = 'magnolia' then 'scripps'
      when lower(channel_name) = 'travel' then 'scripps'
      when lower(channel_name) = 'sundance' then 'amc'
      when lower(channel_name) = 'ryanandfriends' then 'pocketwatch'
      when lower(channel_name) = 'wetv' then 'amc'
      when lower(channel_name) = 'cheddar' then 'cheddar'
      when lower(channel_name) = 'viceland' then 'aetv'
      when lower(channel_name) = 'nicjr' then 'viacom'
      when lower(channel_name) = 'cmtv' then 'viacom'
      when lower(channel_name) = 'own' then 'discovery'
      when lower(channel_name) = 'playerstv' then 'playerstv'
      when lower(channel_name) = 'ae' then 'aetv'
      when lower(channel_name) = 'bloomberg' then 'bloomberg'
      when lower(channel_name) = 'lmn' then 'aetv'
      when lower(channel_name) = 'revry' then 'revry'
      when lower(channel_name) = 'tvland' then 'viacom'
      when lower(channel_name) = 'id' then 'discovery'
      when lower(channel_name) = 'tvone' then 'urbanone'
      when lower(channel_name) = 'gusto' then 'gusto'
      when lower(channel_name) = 'ahc' then 'discovery'
      when lower(channel_name) = 'history' then 'aetv'
      when lower(channel_name) = 'accuweather' then 'accuweather'
      when lower(channel_name) = 'aspire' then 'aspire'
      when lower(channel_name) = '[CONTENT_CHANNEL]' then '[CONTENT_NETWORK]'
      when lower(channel_name) = 'amc' then 'amc'
      when lower(channel_name) = 'crackle' then 'css'
      when lower(channel_name) = 'cleo' then 'urbanone'
      when lower(channel_name) = 'revolt' then 'revolt'
      when lower(channel_name) = 'up' then 'aspire'
      when lower(channel_name) = 'diy' then 'scripps'
      when lower(channel_name) = 'gettv' then 'sony'
      when lower(channel_name) = 'animalplanet' then 'discovery'
      when lower(channel_name) = 'destinationamerica' then 'discovery'
      when lower(channel_name) = 'pocketwatch' then 'pocketwatch'
      when lower(channel_name) = 'bet' then 'viacom'
      when lower(channel_name) = 'cooking' then 'scripps'
      when lower(channel_name) = 'axs' then 'axs'
      when lower(channel_name) = 'discovery' then 'discovery'
      when lower(channel_name) = 'bbca' then 'amc'
      when lower(channel_name) = 'hallmarkdrama' then 'hallmark'
      when lower(channel_name) = 'tlc' then 'discovery'
      when lower(channel_name) = 'hgtv' then 'scripps'
      when lower(channel_name) = 'logo' then 'viacom'
      when lower(channel_name) = 'lifetime' then 'aetv'
      when lower(channel_name) = 'tastemade' then 'tastemade'
      when channel_name is not null then 'ERROR: NETWORK UNKNOWN'
    end                            as network
    , pod_id || ':' || sutured_pid as pods
    , case
      when
        (lower(network) in ('discovery', 'scripps') and lower(asset_type) in ('live', 'dvr')) then 'freewheel'
      else 'publica'
    end                            as ad_server
    , sum(ad_duration)             as ad_fill
  from {{ ref('fct_beacons') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
  {{ dbt_utils.group_by(n=7) }}
)

select
  partition_date
  , platform
  , asset_type
  , network
  , channel
  , ad_server
  , count(1)     as pods_count
  , sum(ad_fill) as ad_fill

from ads
{{ dbt_utils.group_by(n=6) }}
