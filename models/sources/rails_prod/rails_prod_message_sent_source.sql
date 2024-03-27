-- restricting messages until after Jul 24 2020, after which email records external_ids

with

message_sent as (

  select
    user_id             as user_id
    , id                as message_id
    , {{ normalize_id("external_id") }}                     as message_external_id
    , received_at       as received_at
    , event             as message_event
    , channel           as message_channel
    , name              as message_name
    , answers           as answers
    , type              as message_type
    , braze_campaign_id as braze_campaign_id
    , braze_canvas_id   as braze_canvas_id
    , braze_step_id     as braze_step_id
    , braze_variant_id  as braze_variant_id
    , uuid_ts           as loaded_at
    , "timestamp"       as event_timestamp
  from {{ source('rails_prod', 'message_sent') }}
  where received_at > '2020-07-24 00:00:00'
    and user_id is not null -- we also have anonymous user's who receive messages, but we are choosing to exclude them
)

select *
from message_sent
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
