with

app_installs as (

  select *
  from {{ ref('viziotv_prod_application_installed_stage') }}

)

select *
from app_installs
where coalesce(trim(attributed_touch_type), '') != ''
