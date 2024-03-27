{{ config(
    materialized='tuple_incremental'
    , unique_key=['event_date']
    , sort=['playback_session_id', 'event_timestamp']
    , dist='user_id' 
    , tags=["exclude_hourly"]
    , on_schema_change='append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

/*
To get the attribution for playback sessions, we must look back
in the user's journey on the app to identify where their first and last
touch was in the platform before they began playback. To do so, we
create a sequence of a combination of interact events and screen events
in sequence to idenfity what the user did before they began playback.
*/

with

time_series as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('fct_screen_events')
        , ref('dataserver_prod_user_playback_session_map')
        , ref('fct_interact_events')
      ]
      , include=[
          "user_id"
        , "platform"
        , "event_timestamp"
        , "received_at"
        , "view"
        , "screen_name"
        , "collection_id"
        , "collection_index"
        , "playback_session_id"
        , "played_asset_id"
        , "dbt_processed_at"

      ]
    )
  }}

)

, dim_collections as (

  select
    collection_id
    , collection_name
  from {{ ref('dim_collections') }}

)

, time_series_adj as (

  select
    time_series.user_id
    , time_series.view
    , time_series.screen_name
    , time_series.collection_id
    , time_series.collection_index
    , time_series.playback_session_id
    , time_series.played_asset_id
    , dim_collections.collection_name                                as collection_name
    , coalesce(
      time_series.platform
      , {{ get_platform_from_playback_session("time_series.playback_session_id") }}
    )                                                                as platform
    , coalesce(time_series.event_timestamp, time_series.received_at) as event_timestamp
    , case when lower(time_series.view) = ('home') then 'collection: ' || dim_collections.collection_name
      when lower(time_series.view) in ('top', 'guide', 'search', 'saved') then 'screen: ' || lower(time_series.view)
      when time_series.screen_name in ('top', 'guide', 'search', 'saved') then 'screen: ' || time_series.screen_name
    end                                                              as screen_name_adj
    , case when lower(time_series.view) in ('home')
        then time_series.collection_id
    -- Home page collection null for top, guide, search, saved 
    end                                                              as collection_id_adj
    , case when lower(time_series.view) in ('home')
        then time_series.collection_index
    -- Home page collection null for top, guide, search, saved 
    end                                                              as collection_index_adj
  from time_series
  left join dim_collections on time_series.collection_id = dim_collections.collection_id
  where time_series.user_id is not null -- removing anonymous events as there's no use case right now for non-signed in user journeys

)

, results as (

  select
    *
    , date_trunc(
      'day', event_timestamp
    )::date
    as event_date
    , last_value(screen_name_adj ignore nulls)
    -- finds the last view or screen before a playback occured 
      over (partition by user_id, platform order by event_timestamp rows between unbounded preceding and current row)
    as attributed_item_first_touch
    , last_value(collection_id_adj ignore nulls)
    -- finds the last collection before a playback occured 
      over (partition by user_id, platform order by event_timestamp rows between unbounded preceding and current row)
    as collection_id_first_touch
    , last_value(collection_index_adj ignore nulls)
    -- finds the last collection before a playback occured and pull it's index
      over (partition by user_id, platform order by event_timestamp rows between unbounded preceding and current row)
    as collection_index_first_touch

  from time_series_adj

)

select *
from results
where
  event_timestamp between '{{ start_date }}' and '{{ end_date }}'