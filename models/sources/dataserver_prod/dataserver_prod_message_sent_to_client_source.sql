-- restricting messages until after Jul 24 2020, after which email records external_ids

with

message_sent_to_client as (

  select
    user_id       as user_id
    , id          as message_id
    , {{ normalize_id("external_id") }}               as message_external_id
    , event       as message_event
    , uuid_ts     as loaded_at
    , "timestamp" as event_timestamp
  from {{ source('dataserver_prod', 'message_sent_to_client') }}
  where received_at > '2020-07-24 00:00:00'

)

select *
from message_sent_to_client
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
