with source as (

  select *
  from {{ source('rails_prod', 'user_credentialed') }}

)

, renamed as (

  select
    user_id           as user_id
    , "timestamp"     as event_timestamp
    , received_at     as received_at
    , credential_type as credential_type
  from source

)

select * from renamed

{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}