{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='dbt_processed_at'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

rebuffering_start as (

  select
    {{ qoe_columns_select(skip_columns=['position', 'position_ms']) }}
    -- We've seen some overflows with very large negative doubles
    -- being sent by the apple clients on this event. Cast to bigint here
    -- instead.
    , case
      when position < -9223372036854775808
        then null
      else position::bigint
    end                   as position -- noqa: L029
  from {{ ref('ios_prod_rebuffering_start_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from rebuffering_start
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}

{% endif %}