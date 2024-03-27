with

source as (

  select * from {{ source('rails_prod', 'fsm_state_changed') }}

)

, renamed as (

  select
    user_id
    , biller        as subscriber_billing
    , old_fsm_state
    , new_fsm_state as subscriber_state
    , "timestamp"   as state_started_at
  from source

)

select *
from renamed
{%- if target.name != 'prod' %}
  where state_started_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
