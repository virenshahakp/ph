with

rebuffering_end as (

  select * from {{ source('samsung_prod', 'rebuffering_end') }}

)

, renamed as (

  select
    {{ samsung_qoe_source_columns() }}
    , duration_ms -- keeping this column until reports can be migrated to duration
    , duration_ms / 1000.0                    as duration -- noqa: L029
    , position_ms / 1000.0                    as position -- noqa: L029
  from rebuffering_end

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
