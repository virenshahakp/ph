with

rebuffering_end as (

  select * from {{ source('chromecast_prod', 'rebuffering_end') }}

)

, renamed as (

  select
    {{ web_qoe_source_columns(chromecast=True) }}
    , duration
    -- , duration_ms
  from rebuffering_end

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
