with

rebuffering_start as (

  select * from {{ source('viziotv_prod', 'rebuffering_start') }}

)

, renamed as (

  select
    {{ viziotv_qoe_source_columns() }}
    , position_ms / 1000.0                    as position -- noqa: L029
  from rebuffering_start

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
