with

rebuffering_start as (

  select * from {{ source('chromecast_prod', 'rebuffering_start') }}

)

, renamed as (

  select
    {{ web_qoe_source_columns(chromecast=True) }}
  from rebuffering_start

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
