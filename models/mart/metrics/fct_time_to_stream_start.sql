{{ config(materialized='view') }}

with

all_time_to_stream_events as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_time_to_stream_start')
        , ref('androidtv_prod_time_to_stream_start')
        , ref('fire_prod_time_to_stream_start')
        , ref('fire_tv_prod_time_to_stream_start')
        , ref('ios_prod_time_to_stream_start')
        , ref('roku_prod_time_to_stream_start')
        , ref('samsung_prod_time_to_stream_start')
        , ref('tvos_prod_time_to_stream_start')
        , ref('viziotv_prod_time_to_stream_start')
      ]
      , include=[
        "user_id"
        , "platform"
        , "open_time"
        , "open_date"
        , "time_to_stream_start"
      ]
    )
  }}

)

select
  user_id
  , platform
  , open_date
  , open_time
  , time_to_stream_start
from all_time_to_stream_events

