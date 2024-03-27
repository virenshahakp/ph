{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
	, 'partition_hour'
  , 'adomain']
  , dist='adomain'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

select                                                                                                    -- noqa: L034
  publica_bidder_impression_dyn_source.partition_date::date                as partition_date
  , publica_bidder_impression_dyn_source.partition_hour                    as partition_hour
  , date_add(
    'hour'
    , publica_bidder_impression_dyn_source.partition_hour::int
    , publica_bidder_impression_dyn_source.partition_date::timestamp
  )                                                                        as utc_ts
  , dim_publica_platform_map.client_type                                   as platform
  , publica_bidder_impression_dyn_source.content_network                   as network
  , publica_bidder_impression_dyn_source.content_channel                   as channel
  , case
    when publica_bidder_impression_dyn_source.livestream = 1 then 'live'
    when publica_bidder_impression_dyn_source.livestream_str = 0 then 'vod'
    when publica_bidder_impression_dyn_source.livestream_str = 2 then 'dvr'
  end                                                                      as asset_type
  , publica_bidder_impression_dyn_source.bidrequest_device_geo_region      as device_geo_region
  , publica_bidder_impression_dyn_source.bidrequest_device_geo_city        as device_geo_city
  , dim_demand_partner_map.bidder_name                                     as bidder_name
  , dim_demand_partner_map.demand_partner                                  as demand_partner
  , dim_demand_partner_map.fill_type_classification                        as fill_type_classification
  , dim_demand_partner_map.pod_owner                                       as sov
  , publica_bidder_impression_dyn_source.bidder_tier                       as sov__tier
  , replace(publica_bidder_impression_dyn_source.custom_adomain, '''', '') as adomain
  , dim_demand_partner_map.is_paid                                         as is_paid
  , dim_demand_partner_map.is_count                                        as is_count
  , case
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm is null
      or publica_bidder_impression_dyn_source.bidresponse_cpm = 0
      then null
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm > 0
      and publica_bidder_impression_dyn_source.bidresponse_cpm < .50
      then '00 to .50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= .50
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 1
      then '00.50 to 1'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 1
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 1.5
      then '01 to 1.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 1.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 2
      then '01.50 to 2'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 2
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 2.5
      then '02 to 2.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 2.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 3
      then '02.50 to 3'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 3
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 3.5
      then '03 to 3.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 3.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 4
      then '03.50 to 4'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 4
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 4.5
      then '04 to 4.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 4.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 5
      then '04.5 to 5'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 5.5
      then '05 to 5.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 5.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 6
      then '05.50 to 6'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 6
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 6.5
      then '06 to 6.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 6.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 7
      then '06.50 to 7'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 7
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 7.5
      then '07 to 7.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 7.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 8
      then '07.50 to 8'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 8
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 8.5
      then '08 to 8.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 8.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 9
      then '08.50 to 9'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 9
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 9.5
      then '09 to 9.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 9.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 10
      then '09.50 to 10'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 10
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 10.5
      then '10 to 10.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 10.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 11
      then '10.5 to 11'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 11
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 11.5
      then '11 to 11.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 11.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 12
      then '11.50 to 12'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 12
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 12.5
      then '12 to 12.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 12.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 13
      then '12.50 to 13'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 13
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 13.5
      then '13 to 13.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 13.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 14
      then '13.50 to 14'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 14
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 14.5
      then '14 to 14.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 14.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 15
      then '14.50 to 15'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 15
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 15.5
      then '15 to 15.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 15.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 16
      then '15.50 to 16'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 16
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 16.5
      then '16 to 16.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 16.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 17
      then '16.50 to 17'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 17
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 17.5
      then '17 to 17.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 17.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 18
      then '17.5 to 18'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 18
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 18.5
      then '18 to 18.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 18.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 19
      then '18.50 to 19'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 19
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 19.5
      then '19 to 19.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 19.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 20
      then '19.50 to 20'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 20
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 20.5
      then '20 to 20.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 20.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 21
      then '20.50 to 21'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 21
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 21.5
      then '21 to 21.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 21.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 22
      then '21.50 to 22'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 22
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 22.5
      then '22 to 22.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 22.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 23
      then '22.50 to 23'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 23
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 23.5
      then '23 to 23.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 23.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 24
      then '23.50 to 24'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 24
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 24.5
      then '24 to 24.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 24.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 25
      then '24.50 to 25'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 25
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 25.5
      then '25 to 25.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 25.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 26
      then '25.50 to 26'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 26
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 26.5
      then '26 to 26.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 26.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 27
      then '26.50 to 27'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 27
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 27.5
      then '27 to 27.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 27.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 28
      then '27.50 to 28'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 28
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 28.5
      then '28 to 28.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 28.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 29
      then '28.50 to 29'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 29
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 29.5
      then '29 to 29.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 29.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 30
      then '29.50 to 30'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 30
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 30.5
      then '30 to 30.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 30.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 31
      then '30.50 to 31'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 31
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 31.5
      then '31 to 31.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 31.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 32
      then '31.50 to 32'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 32
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 32.5
      then '32 to 32.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 32.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 33
      then '32.50 to 33'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 33
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 33.5
      then '33 to 33.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 33.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 34
      then '33.50 to 34'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 34
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 34.5
      then '34 to 34.50'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 34.5
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 35
      then '34.50 to 35'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 35
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 36
      then '35 to 36'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 36
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 37
      then '36 to 37'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 37
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 38
      then '37 to 38'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 38
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 39
      then '38 to 39'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 39
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 40
      then '39 to 40'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 40
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 41
      then '40 to 41'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 41
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 42
      then '41 to 42'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 42
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 43
      then '42 to 43'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 43
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 44
      then '43 to 44'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 44
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 45
      then '44 to 45'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 45
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 46
      then '45 to 46'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 46
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 47
      then '46 to 47'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 47
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 48
      then '47 to 48'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 48
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 49
      then '48 to 49'
    when
      publica_bidder_impression_dyn_source.bidresponse_cpm >= 49
      and publica_bidder_impression_dyn_source.bidresponse_cpm < 50
      then '49 to 50'
    when publica_bidder_impression_dyn_source.bidresponse_cpm >= 50 then '50+'
  end                                                                      as bid_density
  , case
    when publica_bidder_impression_dyn_source.delivered_pod_duration < 15 then 'less than 15'
    when publica_bidder_impression_dyn_source.delivered_pod_duration = 15 then '15'
    when publica_bidder_impression_dyn_source.delivered_pod_duration between 16 and 29 then '16 to 29'
    when publica_bidder_impression_dyn_source.delivered_pod_duration = 30 then '30'
    when publica_bidder_impression_dyn_source.delivered_pod_duration between 31 and 59 then '31 to 59'
    when publica_bidder_impression_dyn_source.delivered_pod_duration = 60 then '60'
    when publica_bidder_impression_dyn_source.delivered_pod_duration > 60 then 'greater than 60'
  end                                                                      as ad_unit_length
  , dim_demand_partner_map.diversity_type

  , count(1)                                                               as impressions
  , sum(
    case
      when publica_bidder_impression_dyn_source.bidresponse_cpm is not null
        then publica_bidder_impression_dyn_source.bidresponse_cpm::float / 1000
      else 0
    end
  )                                                                        as ad_revenue
  , sum(publica_bidder_impression_dyn_source.delivered_pod_duration)       as delivered_ad_seconds
from {{ ref('publica_bidder_impression_dyn_source') }}
left join {{ ref('dim_demand_partner_map') }}
  on publica_bidder_impression_dyn_source.bidrequest_bids_headerbidder_id = dim_demand_partner_map.bidder_id
left join {{ ref('dim_publica_platform_map') }}
  on publica_bidder_impression_dyn_source.bidrequest_site_id = dim_publica_platform_map.bidrequest_site_id
where publica_bidder_impression_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
{{ dbt_utils.group_by(n=20) }}
