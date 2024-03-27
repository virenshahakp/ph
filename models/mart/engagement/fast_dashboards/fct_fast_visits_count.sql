{{ config(
  materialized='tuple_incremental'
  , unique_key=['report_date']
  , sort=['report_date']
  , on_schema_change = 'append_new_columns'
  , tags=["daily", "exclude_hourly"]
) }}

{% set dates = get_update_dates(1) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

--noqa: disable=L034

with pages as (
  select * from {{ ref('web_prod_pages_source') }}
)

, fast as (
  select * from {{ ref('dim_fast_access_range') }}
)

select
  count(distinct pages.user_id) as visitors_count
  , count(
    distinct case when pages.context_page_path like '%account%' then pages.user_id end
  )                             as account_page_visitors
  , pages.visited_at            as report_date
  --, convert_timezone('US/Pacific', pages.visited_at)::date as report_date
from pages
inner join fast
  on pages.user_id = fast.account_id
    and pages.visited_at between fast.fast_start_time and fast.end_time
where pages.visited_at::date between '{{ start_date }}' and '{{ end_date }}'
--where convert_timezone('US/Pacific', pages.visited_at)::date between '{{ start_date }}' and '{{ end_date }}'
group by 3
