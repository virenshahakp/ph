with

rebuffering_end as (

  select * from {{ source('fire_tv_prod', 'rebuffering_end') }}

)

, renamed as (

  select
    {{ android_qoe_source_columns() }}
    -- , duration
    -- , duration_ms
    , coalesce(duration_ms / 1000.0, duration) as duration
  from rebuffering_end

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
