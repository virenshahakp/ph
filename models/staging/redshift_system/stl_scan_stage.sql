{{ config(
  materialized = 'tuple_incremental'
  , sort = ['query_date']
  , dist = 'even'
  , unique_key = ['query_date']
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(1) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

select *
from {{ ref('stl_scan_source') }}
where starttime::date between '{{ start_date }}' and '{{ end_date }}'
