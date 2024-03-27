with

stream_end as (

  select * from {{ source('ios_prod', 'stream_end') }}

)

, renamed as (

  select
    {{ apple_qoe_source_columns() }}
    , error                       as error_description
    , buffering                   as is_buffering
  from stream_end
)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
