with

source as (

  select * from {{ source('braze_prod', 'campaign_control_group_entered') }}

)

, renamed as (

  select
    user_id                      as user_id
    , "timestamp"                as event_timestamp
    , campaign_id                as experiment_id
    , campaign_name              as experiment_name
    , event                      as event_type
    , uuid_ts                    as loaded_at
    , campaign_id || 'Control'   as variant_id
    , campaign_name || 'Control' as variant_name
  from source

)

select * from renamed
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
