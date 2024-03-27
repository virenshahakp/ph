{{ config(
    materialized='incremental'
    , unique_key=['sutured_pid', 'external_user_id', 'ad_pod_id']
    , dist='external_user_id' 
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , on_schema_change = 'append_new_columns'
) }}


{% set dates = get_update_dates(1) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with psc as (
  select * from dataserver_prod.playback_session_created
)

select
  psc.sutured_pid
  , psc.user_id        as external_user_id
  , tbl_ad_pods.pod_id as ad_pod_id
  , psc.received_at
from psc
inner join {{ ref('tbl_ad_pods') }} on tbl_ad_pods.sutured_pid = psc.sutured_pid
where psc.received_at::date between '{{ start_date }}' and '{{ end_date }}'
  and tbl_ad_pods.event_timestamp::date
  between dateadd('day', -1, '{{ start_date }}') and dateadd('day', 1, '{{ end_date }}')
  and psc.is_new_session is true --this is designed to prevent duplication on pid/pod_id
group by 1, 2, 3, 4
