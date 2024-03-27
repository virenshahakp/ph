{{ config(
materialized='tuple_incremental'
, sort=['partition_date']
, tags=["dai", "exclude_hourly", "exclude_daily"]
, unique_key = ['partition_date']
, on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with duplicates as (
  select
    (year || '-' || month || '-' || day)::date       as partition_date
    , {{ pod_instance_id(source = 'ad_pods') }}      as pod_instance_id
  from {{ source('spectrum_dai', 'ad_pods') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
  qualify row_number() over (partition by partition_date, pod_instance_id) > 1
)

, dates as (
  select observation_date::date as partition_date
  from {{ ref('dim_dates') }}
  where observation_date::date between '{{ start_date }}' and '{{ end_date }}'
)

select
  dates.partition_date::date
  , sum(case when duplicates.pod_instance_id is not null then 1 else 0 end) as duplicates
from dates
left join duplicates on dates.partition_date = duplicates.partition_date
{{ dbt_utils.group_by(n=1) }}
