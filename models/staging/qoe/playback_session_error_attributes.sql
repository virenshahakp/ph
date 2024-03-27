{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort='received_at'
  )
}}

with all_errors as (

  select
    playback_session_id
    , error_code
    , error_description
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , received_at
    , row_number() over (
      partition by playback_session_id
      order by event_timestamp asc
    ) as error_index
  from {{ ref('all_platforms_stream_errors') }}
  {%- if is_incremental() %}
    where received_at >= {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
  {%- endif %}

)

select
  playback_session_id
  , error_code
  , error_description
  , error_philo_code
  , error_detailed_name
  , error_http_status_code
  , received_at
from all_errors
where error_index = 1
