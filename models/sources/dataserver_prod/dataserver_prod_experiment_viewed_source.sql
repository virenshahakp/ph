with

source as (

  select * from {{ source('dataserver_prod', 'experiment_viewed') }}

)

, renamed as (

  select
    user_id                            as anonymous_id
    , user_id                          as user_id
    , variant                          as variant
    , "timestamp"                      as event_timestamp
    , uuid_ts                          as loaded_at
    -- remove apostrophes (chr(39)) from experiment names
    , replace(experiment, chr(39), '') as experiment_name
  from source

)

select * from renamed
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}