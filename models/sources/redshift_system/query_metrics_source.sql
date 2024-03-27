with

query_metrics as (
  select *
  from {{ source('system', 'svl_query_metrics_summary') }}
)

select * from query_metrics