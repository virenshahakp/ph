with

pg_class as (
  -- oid doesn't exist unless you explicitly select it ¯\_(ツ)_/¯
  select
    *
    , "oid"
  from {{ source('system', 'pg_class') }}
)

select * from pg_class