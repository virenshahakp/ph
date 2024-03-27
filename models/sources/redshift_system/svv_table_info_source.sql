select
  database
  , schema
  , "table"
  , encoded
  , diststyle
  , sortkey1
  , max_varchar
  , sortkey1_enc
  , sortkey_num
  , pct_used
  , empty
  , unsorted
  , stats_off
  , tbl_rows
  , size
  , skew_sortkey1
  , skew_rows
  , estimated_visible_rows
  , risk_event
  , vacuum_sort_benefit
  , table_id::int::varchar
  , getdate() as dbt_updated_at
from {{ source('system', 'svv_table_info') }}
where {{ include_redshift_prod_schemas() }}


