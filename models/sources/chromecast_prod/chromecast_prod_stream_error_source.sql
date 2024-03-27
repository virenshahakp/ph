with

stream_error as (

  select * from {{ source('chromecast_prod', 'stream_error') }}

)

, renamed as (

  select
    {{ web_qoe_source_columns(chromecast=True) }}
    , code                        as raw_error_code
    , null                        as error_description
    , philo_code                  as error_philo_code
    , detailed_error_name         as error_detailed_name
    , http_status_code            as error_http_status_code
  from stream_error
)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
