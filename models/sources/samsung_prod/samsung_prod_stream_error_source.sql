with

stream_error as (

  select * from {{ source('samsung_prod', 'stream_error') }}

)

, renamed as (

  select
    {{ samsung_qoe_source_columns() }}
    , code                                    as raw_error_code
    , null::varchar(512)                      as error_description
    , lower(philo_code)                       as error_philo_code
    , detailed_error_name                     as error_detailed_name
    , http_status_code                        as error_http_status_code
    , position_ms / 1000.0                    as position -- noqa: L029
  from stream_error

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
