with

stream_start as (

  select * from {{ source('samsung_prod', 'stream_start') }}

)

, renamed as (

  select
    {{ samsung_qoe_source_columns() }}
    , duration_ms / 1000.0                    as duration
  -- , position    as position -- noqa: L029
  from stream_start

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
