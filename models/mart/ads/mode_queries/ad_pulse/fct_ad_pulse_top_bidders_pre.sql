{{ config(
    materialized='tuple_incremental'
    , unique_key=['report_timezone'
        ,'report_date']    
    , sort=['report_timezone'
    , 'report_date'
    , 'is_paid'
    , 'bidder_name'
    ]
    , dist='even'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_schema_change = 'append_new_columns'
) }}

--bidders

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}



with dates as (
  select *
  from {{ ref('dim_dates') }} 
)

, raw_data as (
  select
    report_timezone
    , report_time::date as report_date
    , bidder_name
    , is_paid
    , sum(impressions) as impressions
    , sum(ad_revenue) as ad_revenue
    , sum(ad_seconds) as ad_seconds
  from  {{ ref('fct_ad_pulse_agg') }}
  where
    report_time::date between  '{{ start_date }}' and '{{ end_date }}'
    and is_count = 'yes'
  {{ dbt_utils.group_by(n=4) }}
)

, unique_values as (
  select distinct 
    bidder_name
    , is_paid
    , report_timezone
  from raw_data
)

--we need to make sure all bidders don't have missing dates, otherwise the rolling calculations will be incorrect 
, data_spine as (
  select
    dates.observation_date::date
    , unique_values.bidder_name
    , unique_values.is_paid
    , unique_values.report_timezone
  from dates
  cross join unique_values
  where dates.observation_date between  '{{ start_date }}' and '{{ end_date }}'
)

select
  data_spine.report_timezone
  , data_spine.observation_date as report_date
  , data_spine.bidder_name
  , data_spine.is_paid
  , raw_data.impressions
  , raw_data.ad_revenue
  , raw_data.ad_seconds
from data_spine
left outer join raw_data on ( raw_data.report_date = data_spine.observation_date
  and raw_data.bidder_name = data_spine.bidder_name
  and raw_data.is_paid = data_spine.is_paid
  and raw_data.report_timezone = data_spine.report_timezone
  )

    
