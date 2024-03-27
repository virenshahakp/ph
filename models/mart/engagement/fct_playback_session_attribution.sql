{{ config(
    materialized='tuple_incremental'
    , unique_key=['received_day']
    , sort=['received_at']
    , dist='playback_session_id' 
    , tags=["exclude_hourly"]
    , on_schema_change='append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with

playback_sessions as (

  select
    *
    -- add a unique day key for efficient incremental processing
    , date_trunc('day', received_at) as received_day
  from {{ ref('dataserver_prod_user_playback_session_map') }}

)

/*
To attribute a playback session to their user journey, we find the first touch
where the user was on the app before they began playback.
Methodology for attribution:
https://www.notion.so/philoinc/Home-Page-Play-Attribution-34e7a571f5e940d5853267d2bc528ba6
*/
, user_screen_journeys as (

  select
    playback_session_id
    , attributed_item_first_touch
    , case when attributed_item_first_touch like 'collection%' then collection_id_first_touch end as collection_id_first_touch
    , case
      when attributed_item_first_touch like 'collection%' then collection_index_first_touch
    end                                                                                           as collection_index_first_touch
  from {{ ref('fct_user_screen_journey') }}
  where playback_session_id is not null

)

, results as (

  select
    playback_sessions.*
    , user_screen_journeys.collection_id_first_touch -- null when attributed_item_first_touch is not a collection
    , user_screen_journeys.attributed_item_first_touch
    , user_screen_journeys.collection_index_first_touch
  from playback_sessions
  left join user_screen_journeys
    on playback_sessions.playback_session_id = user_screen_journeys.playback_session_id

)

select *
from results
where
  received_at between '{{ start_date }}' and '{{ end_date }}'
