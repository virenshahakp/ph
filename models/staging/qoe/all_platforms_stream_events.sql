{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , dist='playback_session_id'
    , sort=['received_at', 'event_timestamp', 'playback_session_id']
    , sort_type='interleaved'
    , on_schema_change='append_new_columns'
  )
}}

{%- set max_dbt_processed_at = incremental_max_value('dbt_processed_at') %}

with

events as (

  select *
  from
    {{ dbt_utils.union_relations(
    relations=[
      ref('all_platforms_stream_starts')
      , ref('all_platforms_stream_ends')
      , ref('all_platforms_stream_errors')
      , ref('all_platforms_rebuffering_starts')
      , ref('all_platforms_rebuffering_ends')
    ]
    , include=qoe_columns(additional_columns=[
      'duration'
      , 'error_code'
      , 'error_description'
      , 'error_philo_code'
      , 'error_detailed_name'
      , 'error_http_status_code'
      , 'is_buffering'
      , 'is_errored'
      , 'platform'
      , 'event_id'
      , 'played_asset_id'
      , 'requested_asset_id'
      , 'dbt_processed_at'
      ])
    ) }}
)

select
  {{ columns_select(qoe_columns(additional_columns=[
        'duration'
        , 'error_code'
        , 'error_description'
        , 'error_philo_code'
        , 'error_detailed_name'
        , 'error_http_status_code'
        , 'is_buffering'
        , 'is_errored'
        , 'platform'
        , 'event_id'
        , 'played_asset_id'
        , 'requested_asset_id'
      ])
      , ['position_ms']
    )
  }}
  , sysdate                as dbt_processed_at
from events
{%- if is_incremental() %}
  where dbt_processed_at > {{ max_dbt_processed_at }}
{%- endif %}