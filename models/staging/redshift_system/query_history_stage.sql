{{ config(
  materialized = 'incremental'
  , dist = 'transaction_id'
  , sort = ['start_time']
  , unique_key = ['query_id']
  , tags = ["daily", "exclude_hourly"]
  , on_schema_change = 'append_new_columns'
) }}

{%- set max_end_time = incremental_max_value('end_time') %}

select *
from {{ ref('query_history_source') }}
{%- if is_incremental() %}
  where end_time > {{ max_end_time }}
{% endif %}