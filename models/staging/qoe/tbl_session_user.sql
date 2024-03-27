{{ config(
    materialized='incremental'
    , unique_key=['hashed_session_id']
    , dist='hashed_session_id' 
    , tags=["dai", "exclude_hourly", "exclude_daily"]
) }}


{% set dates = get_update_dates(1) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


select
  user_id as external_user_id
  , hashed_session_id
from {{ ref('all_platforms_stream_starts') }}
where dbt_processed_at::date between '{{ start_date }}' and '{{ end_date }}'
  and hashed_session_id is not null
group by 1, 2
