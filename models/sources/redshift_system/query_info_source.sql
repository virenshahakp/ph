with

query_info as (
  select *
  from {{ source('system', 'stl_query') }}
)

select * from query_info
