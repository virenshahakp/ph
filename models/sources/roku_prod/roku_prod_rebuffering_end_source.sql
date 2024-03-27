with

rebuffering_end as (

  select * from {{ source('roku_prod', 'rebuffering_end') }}

)

, renamed as (

  select
    {{ roku_qoe_source_columns() }}
    , coalesce(duration_ms / 1000.0, duration) as duration
  from rebuffering_end

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
