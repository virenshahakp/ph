select
  *
  , starttime::date as query_date
  , getdate()       as dbt_updated_at
from {{ source('system', 'stl_scan') }}
