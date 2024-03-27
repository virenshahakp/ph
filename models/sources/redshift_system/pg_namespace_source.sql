with

pg_namespace as (
  -- oid doesn't exist unless you explicitly select it ¯\_(ツ)_/¯
  select
    *
    , "oid"
  from {{ source('system', 'pg_namespace') }}
)

select * from pg_namespace