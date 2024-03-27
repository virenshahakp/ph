with

appsflyer as (

  select * from {{ ref('spectrum_appsflyer_stage') }}

)

select *
from appsflyer
where event_name = 'install'
  and country_code = 'US'
  -- filtering out the reinstalls
  and conversion_type not in ('REINSTALL_ORGANIC', 'REINSTALL_NON_ORGANIC')