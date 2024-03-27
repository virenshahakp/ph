with

query_history as (
  select
    *
    , getdate() as dbt_updated_at
  from {{ source('system', 'sys_query_history') }}
  where query_type != 'UTILITY'
)

select * from query_history