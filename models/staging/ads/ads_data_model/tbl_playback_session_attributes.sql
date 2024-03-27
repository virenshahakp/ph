{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set end_date_alias = dates.end_date_alias %}

{{ config(
    materialized='table'
    , alias='tbl_playback_session_attributes_' + end_date_alias
    , sort=['sutured_pid']
    , dist='sutured_pid'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
) }}

with properties as (
  select distinct
    sutured_pid
    , device_name
    , platform
    , dma
    , os_version
    , app_version
  from {{ ref('fct_playback_sessions') }}
  where created_at::date between '{{ start_date }}'::date - interval '1 day'
    and '{{ end_date }}'::date + interval '1 day'
)

select
  sutured_pid
  , device_name
  , platform
  , dma
  , os_version
  , app_version
from properties
group by 1, 2, 3, 4, 5, 6
having count(1) = 1 --workaround to exlcude philo-connect

