with

svv_transactions as (
  select * from {{ source('system', 'svv_transactions') }}
)

select * from svv_transactions