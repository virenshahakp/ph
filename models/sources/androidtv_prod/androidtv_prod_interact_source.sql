with

interact as (

  select * from {{ source('androidtv_prod', 'interact') }}

)

, renamed as (

  select
    {{ android_interact_source_columns() }}
  from interact

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
