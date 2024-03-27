with

daily_channel_performance as (

  select * from {{ ref('uploads_daily_marketing_channel_performance_stage') }}

)

select
  ad
  , ad_date
  , spend
  , impressions
  , conversions
  , case
    when (channel ilike 'google' and ad ilike '%perfmax%')
      or (channel ilike 'google' and ad ilike '%gmail%') 
      or (channel ilike 'google_discovery' )
      or (channel ilike 'google_app')
      then 'acquisition_other'
    else channel
  end as channel
from daily_channel_performance
where channel ilike 'google'
  or (channel ilike 'google_discovery' )
  or (channel ilike 'google_app')
 