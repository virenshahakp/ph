with

stream_start as (

  select * from {{ source('chromecast_prod', 'stream_start') }}

)

, renamed as (

  select
    {{ web_qoe_source_columns(chromecast=True) }}
    , duration
    -- , duration_ms
    , manifest_fetch_time
    , manifest_parsed_time
    , event_text
    , md5(nullif(trim(context_user_agent), '')) as context_user_agent_id
  from stream_start

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
