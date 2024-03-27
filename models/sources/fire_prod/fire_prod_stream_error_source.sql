with

stream_error as (

  select * from {{ source('fire_prod', 'stream_error') }}

)

, renamed as (

  select
    {{ android_qoe_source_columns() }}
    , code                        as error_code
    , description                 as error_description
    , philo_code                  as error_philo_code
    , detailed_error_name         as error_detailed_name
    , null                        as error_http_status_code
  from stream_error
)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
