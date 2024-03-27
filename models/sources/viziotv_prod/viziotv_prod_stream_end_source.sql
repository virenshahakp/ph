with

stream_end as (

  select * from {{ source('viziotv_prod', 'stream_end') }}

)

, renamed as (

  select
    {{ viziotv_qoe_source_columns() }}
    -- , error              as error_description
    , buffering                               as is_buffering
    , position_ms / 1000.0                    as position -- noqa: L029
  from stream_end
)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
