{{ config(
  materialized='incremental'
  , unique_key='event_id'
  , dist='user_id'
  , sort=['event_timestamp', 'requested_asset_id', 'played_asset_id']
  , on_schema_change='sync_all_columns'
  )
}}

{%- set max_dbt_processed_at = incremental_max_value('dbt_processed_at') %}

with

stream_starts as (

  -- explicitly select the columns for the incremental union
  select
    all_platforms_stream_starts.event_id
    , all_platforms_stream_starts.event_timestamp
    , all_platforms_stream_starts.received_at
    , all_platforms_stream_starts.client_ip
    , all_platforms_stream_starts.hashed_session_id
    , all_platforms_stream_starts.playback_session_id
    , all_platforms_stream_starts.adapted_bitrate
    , all_platforms_stream_starts.duration
    , all_platforms_stream_starts.position                                                    as position_start
    , all_platforms_stream_starts.platform
    , trunc(all_platforms_stream_starts.event_timestamp)                                      as event_date

    {% if is_incremental() %}
      -- update event with new information if new data is not null, calculate the row number in the new batch for new events
      , existing.stream_number                                                                as existing_stream_number
      , coalesce(all_platforms_stream_starts.user_id, existing.user_id)                       as user_id
      , coalesce(all_platforms_stream_starts.dbt_processed_at, existing.dbt_processed_at)     as dbt_processed_at
      , coalesce(all_platforms_stream_starts.played_asset_id, existing.played_asset_id)       as played_asset_id
      , coalesce(all_platforms_stream_starts.requested_asset_id, existing.requested_asset_id) as requested_asset_id
      , coalesce(all_platforms_stream_starts.played_asset_id, existing.asset_id)              as asset_id
      , row_number() over (
        partition by all_platforms_stream_starts.user_id
        -- event_id in case of ties
        order by all_platforms_stream_starts.event_timestamp asc, all_platforms_stream_starts.event_id asc
      )                                                                                       as stream_number

    {% else %}
      , null                                                                                  as existing_stream_number
      , all_platforms_stream_starts.user_id
      , all_platforms_stream_starts.dbt_processed_at
      , all_platforms_stream_starts.played_asset_id
      , all_platforms_stream_starts.requested_asset_id
      , coalesce(
        all_platforms_stream_starts.played_asset_id
        , all_platforms_stream_starts.requested_asset_id
      )                                                                                       as asset_id
      , row_number() over (
        partition by all_platforms_stream_starts.user_id
        -- event_id in case of ties
        order by all_platforms_stream_starts.event_timestamp asc, all_platforms_stream_starts.event_id asc
      )                                                                                       as stream_number
    {% endif %}

  from
    {{ ref('all_platforms_stream_starts') }}
  {%- if is_incremental() %}
  --noqa: disable=L031
  left join {{ this }} as existing
    on (
      all_platforms_stream_starts.user_id = existing.user_id -- help redshift use the dist_key
      and all_platforms_stream_starts.event_id = existing.event_id
      )
  where all_platforms_stream_starts.dbt_processed_at > {{ max_dbt_processed_at }}
  {%- endif %}

)

, guide as (

  select * from {{ ref('dim_guide_metadata') }}

)

, user_max_values as (

  {% if is_incremental() %}

    select
      user_id
      , max(stream_number) as max_stream_number
    from {{ this }}
    where user_id in (
        select user_id from stream_starts
      )
    {{ dbt_utils.group_by(n=1) }}

  {% else %}

    select
      null as user_id
      , 0  as max_stream_number

  {% endif %}

)

select
  stream_starts.user_id
  , stream_starts.event_id
  , stream_starts.event_timestamp
  , stream_starts.received_at
  , stream_starts.client_ip
  , stream_starts.hashed_session_id
  , stream_starts.playback_session_id
  , stream_starts.played_asset_id
  , stream_starts.asset_id
  , stream_starts.requested_asset_id
  , stream_starts.adapted_bitrate
  , stream_starts.duration
  , stream_starts.position_start
  , stream_starts.platform
  , stream_starts.dbt_processed_at
  , played_content.asset_type    as played_asset_type
  , requested_content.asset_type as requested_asset_type
  , stream_starts.event_date
  , coalesce(
    stream_starts.existing_stream_number
    , stream_starts.stream_number + coalesce(user_max_values.max_stream_number, 0)
  )                              as stream_number
from stream_starts
left join guide as played_content on (stream_starts.played_asset_id = played_content.asset_id)
left join guide as requested_content on (stream_starts.requested_asset_id = requested_content.asset_id)
left join user_max_values on (stream_starts.user_id = user_max_values.user_id)
