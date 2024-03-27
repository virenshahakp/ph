{{ config(
materialized='tuple_incremental'
, sort=['partition_date','sutured_pid']
, dist='sutured_pid'
, tags=["dai", "exclude_hourly", "exclude_daily"]
, unique_key = ['partition_date']
, on_schema_change = 'append_new_columns'
) }}

-- noqa: disable=LT01

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

select
  partition_date
  , sutured_pid
  , user_id
  , sum(cpm_price / 1000.0)      as ad_revenue_dollars
  , sum(unique_impression_count) as ad_impressions
  , sum(unique_complete_count)   as completed_ad_impressions
from {{ ref('fct_vast_ads_enriched') }}
where partition_date between '{{ start_date }}' and '{{ end_date }}'
  and impression_count >= 1 --limit to records where we have at least one impression as it is required for revenue
{{ dbt_utils.group_by(n=3) }}