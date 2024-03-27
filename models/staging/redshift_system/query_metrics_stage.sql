{{ config(
  materialized = 'incremental'
  , dist = 'transaction_id'
  , sort = ['start_time']
  , unique_key = ['query']
  , tags = ["daily", "exclude_hourly"]
  , on_schema_change = 'append_new_columns'
) }}

{%- set max_end_time = incremental_max_value('end_time') %}

with

query_metrics as (
  select *
  from {{ ref('query_metrics_source') }}
)

, query_info as (
  select *
  from {{ ref('query_info_source') }}
  {%- if is_incremental() %}
    where endtime > {{ max_end_time }}
  {% endif %}
)

select
  query_metrics.*
  , query_info.xid       as transaction_id
  , query_info.pid
  , query_info.starttime as start_time
  , query_info.endtime   as end_time
  , query_info.concurrency_scaling_status
  , getdate()            as dbt_updated_at
from query_info
join query_metrics
  on query_info.query = query_metrics.query
