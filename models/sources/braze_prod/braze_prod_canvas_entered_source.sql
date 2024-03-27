with

source as (

  select * from {{ source('braze_prod', 'canvas_entered') }}

)

, renamed as (

  select
    user_id                 as user_id
    , "timestamp"           as event_timestamp
    , canvas_id             as experiment_id
    , canvas_name           as experiment_name
    , canvas_variation_id   as variant_id
    , canvas_variation_name as variant_name
    , event                 as event_type
    , uuid_ts               as loaded_at
  from source

)

select * from renamed
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}