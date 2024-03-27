with

app_installs as (

  select *
  from {{ ref('roku_prod_launch_stage') }}

)

select *
from app_installs
where coalesce(trim(attributed_touch_type), '') != ''
