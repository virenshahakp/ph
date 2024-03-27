with 

reports as (

  select * from {{ ref('google_ads_campaign_performance_reports_source') }}

)

select * from reports