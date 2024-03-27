with

source as (

  select * from {{ source('rails_prod', 'fast_plan_state_changed') }}

)

, renamed as (

  select
    user_id
    , old_biller
    , new_biller           as subscriber_billing
    , reason               as state_change_reason
    , user_is_credentialed as is_user_credentialed
    , old_fast_plan_state
    , new_fast_plan_state  as fast_plan_state
    , old_fsm_state
    , new_fsm_state        as subscriber_state
    , "timestamp"          as state_started_at
  from source

)

select *
from renamed
{%- if target.name != 'prod' %}
  where state_started_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
