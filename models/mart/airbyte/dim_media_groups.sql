{{ config(
    materialized = 'table'
    , dist = 'ALL'
    , sort = 'is_on_philo'
) }}


select
  callsign
  , channel_name
  , display_name
  , is_on_philo
  , media_group
  , rate
  , proposed_rate
  , rate2021
from {{ ref('airbyte_media_groups_stage') }}
