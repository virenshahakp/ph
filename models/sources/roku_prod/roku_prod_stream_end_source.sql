with

stream_end as (

  select * from {{ source('roku_prod', 'stream_end') }}

)

, renamed as (

  select
    {{ roku_qoe_source_columns() }}
    , error                      as is_errored
    , buffering                  as is_buffering
  from stream_end
)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
