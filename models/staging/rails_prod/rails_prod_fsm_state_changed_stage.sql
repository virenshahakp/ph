{{ 
  config(
    materialized='tuple_incremental'
    , unique_key=['user_id', 'state_started_at', 'subscriber_billing']
    , dist='user_id'
    , sort=['state_started_at', 'subscriber_billing']
  )
}}
with 

{%- set max_started_at = incremental_max_value('state_started_at') %}

fsm_state as (

  select *
  from {{ ref('rails_prod_fsm_state_changed_source') }}
  {% if is_incremental() %}
    where state_started_at > '{{ max_started_at }}'
  {% endif %}

)

{% if is_incremental() %}
, active_records as (

    select 
      user_id
      , subscriber_billing
      , old_fsm_state
      , subscriber_state
      , state_started_at
    from {{ this }} 
    where is_active is true
      and user_id in (select user_id from fsm_state)

)
{% endif %}

, combine_new_and_updates as (

  select
    user_id
    , subscriber_billing
    , old_fsm_state
    , subscriber_state
    , state_started_at
  from fsm_state
  {% if is_incremental() %}
    union all
    select
      user_id
      , subscriber_billing
      , old_fsm_state
      , subscriber_state
      , state_started_at
    from active_records
  {% endif %}

)

, valid_until as (

  select
    combine_new_and_updates.*
    , lead(state_started_at) over (partition by user_id order by state_started_at asc) as next_timestamp
  from combine_new_and_updates

)

select
  user_id
  , subscriber_billing
  , subscriber_state
  , state_started_at
  , next_timestamp         as state_ended_at
  , next_timestamp is null as is_active
from valid_until
