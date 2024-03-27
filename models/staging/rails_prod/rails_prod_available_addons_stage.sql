{{
    config(
      re_data_monitored=false
    )
}}

with

available_addons as (

  select
    package
    , 'addon' as package_type 
    , min(received_at) as earliest_added
    , max(received_at) as latest_added
  from {{ ref('rails_prod_package_added_source') }}
  where package like 'epix%' or package like 'movie%' or package like 'starz%'
  group by 1

)

select *
from available_addons
