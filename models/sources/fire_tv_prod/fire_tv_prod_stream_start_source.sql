with

stream_start as (

  select * from {{ source('fire_tv_prod', 'stream_start') }}

)

, renamed as (

  select
    {{ android_qoe_source_columns() }}
    , coalesce(duration_ms / 1000.0, duration)                                     as duration
  from stream_start

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
