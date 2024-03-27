with

stream_error as (

  select * from {{ source('roku_prod', 'stream_error') }}

)

, renamed as (

  select
    {{ roku_qoe_source_columns() }}
    , code                       as error_code
    , description                as error_description
    , philocode                  as error_philo_code
    , detailederrorname          as error_detailed_name
    , httpstatuscode             as error_http_status_code
  from stream_error
)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
