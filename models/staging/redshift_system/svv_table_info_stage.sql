{{ config(
materialized = 'incremental'
, sort = ['dbt_updated_at']
, dist = 'even'
, tags = ["daily", "exclude_hourly"]
, on_schema_change = 'append_new_columns'
) }}

select *
from {{ ref('svv_table_info_source') }}