{{ config(materialized='view') }}
with

web_and_chromecast_manifest as (

  {{ dbt_utils.union_relations(
      relations=[
          ref('chromecast_prod_stream_start_stage')
        , ref('web_prod_stream_start_stage')
      ]
    )
  }}

)


, add_platform as (

  select
    web_and_chromecast_manifest.*
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from web_and_chromecast_manifest
  where playback_session_id is not null

)

select 
  user_id
  , asset_id
  , playback_session_id
  , platform
  , event_timestamp
  , received_at
  , loaded_at
  , event_text
  , duration
  , manifest_fetch_time
  , manifest_parsed_time
  , context_user_agent_id
from add_platform
where manifest_fetch_time is not null
  and manifest_parsed_time is not null

