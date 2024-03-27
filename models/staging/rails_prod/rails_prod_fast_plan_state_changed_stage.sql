with

fast_plan as (

  select * from {{ ref('rails_prod_fast_plan_state_changed_source') }}

)

select * from fast_plan