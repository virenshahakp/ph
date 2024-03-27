with

pg_locks as (
  select * from {{ source('system', 'pg_locks') }}
)

select * from pg_locks