{{ config(
  materialized = 'incremental'
  , dist = 'transaction_id'
  , sort = ['txn_start']
  , unique_key = ['transaction_id']
  , tags = ["hourly_snapshots", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}

with

pg_locks as (
  select * from {{ ref('pg_locks_source') }}
)

, svv_transactions as (
  select * from {{ ref('svv_transactions_source') }}
)

, pg_class as (
  select * from {{ ref('pg_class_source') }}
)

, pg_namespace as (
  select * from {{ ref('pg_namespace_source') }}
)

, locks as (
  select
    svv_transactions.xid                    as transaction_id
    , pg_locks.pid
    , svv_transactions.txn_owner            as username
    , svv_transactions.relation
    , pg_locks.mode
    , pg_locks.granted
    , svv_transactions.lockable_object_type as obj_type
    , svv_transactions.txn_start
    , trim(pg_namespace.nspname)            as schema_name
    , trim(pg_class.relname)                as object_name
  from pg_locks
  inner join svv_transactions
    on pg_locks.pid = svv_transactions.pid
      and pg_locks.relation = svv_transactions.relation
      and svv_transactions.lockable_object_type is not null
  left join pg_class
    on svv_transactions.relation = pg_class.oid
  left join pg_namespace
    on pg_class.relnamespace = pg_namespace.oid
)

, ungranted_locks as (
  select
    relation
    , mode
    , listagg(pid, ',') as pid_list
    , count(1)          as num_blocking
  from locks
  where granted is false
  {{ dbt_utils.group_by(n=2) }}
)

, ungranted_blocking as (
  select
    locks.transaction_id
    , locks.pid
    , locks.username
    , locks.relation
    , locks.schema_name
    , locks.object_name
    , locks.mode
    , locks.granted
    , locks.obj_type
    , locks.txn_start
    , ungranted_locks.num_blocking
    , ungranted_locks.pid_list
    , getdate() as dbt_updated_at
  from locks
  left outer join ungranted_locks
    on locks.relation = ungranted_locks.relation
      and locks.granted is true
      and (
        locks.mode like '%Exclusive%'
        or (locks.mode like '%Share%' and ungranted_locks.mode like '%ExclusiveLock' and ungranted_locks.mode not like '%Share%')
      )
  where locks.granted is false
)

select distinct * from ungranted_blocking