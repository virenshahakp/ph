{{ config(
    materialized='tuple_incremental'
    , unique_key=['report_timezone'
      ,'report_date'
      ]
    , sort=['report_timezone'
        , 'report_date'
    ]
    , dist='even'
    , full_refresh = false
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

select
  report_timezone
  , report_time::date as report_date
  , sum(case when is_paid = 'no' and is_count = 'yes' then impressions else 0 end) as unpaid_impressions
  , sum(case when is_paid = 'yes' then impressions else 0 end) as paid_impressions
  , unpaid_impressions::float / (paid_impressions::float + unpaid_impressions::float) as unpaid_impressions_percent
  , sum(impressions) as impressions
  , sum(ad_revenue) as ad_revenue
  , sum(ad_seconds) as ad_unit_length
from {{ ref('fct_ad_pulse_agg') }}
where
  report_time::date between '{{ start_date }}' and '{{ end_date }}'
  and is_count = 'yes'
{{ dbt_utils.group_by(n=2) }}


