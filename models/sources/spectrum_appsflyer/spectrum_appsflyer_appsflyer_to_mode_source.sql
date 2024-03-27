with

source as (

  select * from {{ source('spectrum_appsflyer', 'appsflyer_to_mode') }}

)

, renamed as (

  select
    attributed_touch_type
    , attributed_touch_time
    , event_name
    , campaign
    , conversion_type
    , af_c_id        as campaign_id
    , af_adset       as adset
    , af_adset_id    as adset_id
    , af_ad          as ad
    , af_ad_id       as ad_id
    , af_ad_type     as ad_type
    , af_siteid      as site_id
    , appsflyer_id
    , advertising_id as context_device_advertising_id
    , idfa
    , idfv           as device_id
    , media_source
    , af_channel     as channel
    , af_keywords    as keywords
    , country_code
    , app_id
    , case when app_id in ('G22223020133', '196460', 'vzphilo') then to_timestamp(install_time, 'DD/MM/YYYY HH24:MI:SS')
      when
        app_id in ('com.philo.philo-Amazon', 'com.philo.philo.google', 'id1248646044')
        then to_timestamp(install_time, 'YYYY-MM-DD HH24:MI:SS')
    end              as install_time
    , case when app_id in ('G22223020133', '196460', 'vzphilo') then to_timestamp(event_time, 'DD/MM/YYYY HH24:MI:SS')
      when
        app_id in ('com.philo.philo-Amazon', 'com.philo.philo.google', 'id1248646044')
        then to_timestamp(event_time, 'YYYY-MM-DD HH24:MI:SS')
    end              as event_time
  from source
  where event_name = 'install'
    and country_code = 'US'
    and conversion_type not in ('REINSTALL_ORGANIC', 'REINSTALL_NON_ORGANIC')
)

select * from renamed