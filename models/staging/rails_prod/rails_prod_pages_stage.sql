{{
    config(
      re_data_time_filter='visited_at'
    )
}}

with 

pages as (

  select * from {{ ref('rails_prod_pages_source') }}

)

select 
  event_id
  , anonymous_id
  , context_ip
  , context_campaign_source
  , context_campaign_name
  , context_campaign_term
  , context_campaign_medium
  , context_page_referrer
  , context_user_agent
  , context_page_path
  , context_campaign_content
  , null as context_campaign_content_id
  , url
  , visited_at
  , coupon_code

  -- manage all the campaign specific logic here
  , null                      as reference
  , case
    when 
      ( 
        pages.context_page_path = '/guhhatl' 
        and pages.received_at < '2018-10-23 16:28:15'
      )
      or (
        pages.context_page_path = '/unbox'
        and pages.received_at < '2019-01-04 23:35:00'
      )
      or (
        lower(pages.context_page_path) = '/ryan' 
        and pages.received_at < '2019-03-06 16:00:00' 
        and pages.received_at > '2019-03-05 00:00:00'
      )
      or (
        lower(pages.context_page_path) = '/kcas' 
        and pages.received_at < '2019-03-21 00:00:00' 
        and pages.received_at > '2019-03-26 00:00:00'
      )
      or (
        pages.context_campaign_term in ('philo-1893', 'philo-1894', 'philo-1895', 'philo-1896')
        or pages.context_campaign_content in ('blackmusicmonthsavethedate', 'betawards', '2019betawards')
        or pages.path in ('/lala', '/kendall')
      )
      then 1
  end as priority
  , case
    when 
      ( 
        pages.context_page_path = '/guhhatl' 
        and pages.received_at < '2018-10-23 16:28:15'
      )
      or (
        pages.context_page_path = '/unbox'
        and pages.received_at < '2019-01-04 23:35:00'
      )
      or (
        lower(pages.context_page_path) = '/ryan' 
        and pages.received_at < '2019-03-06 16:00:00' 
        and pages.received_at > '2019-03-05 00:00:00'
      )
      or (
        lower(pages.context_page_path) = '/kcas' 
        and pages.received_at < '2019-03-21 00:00:00' 
        and pages.received_at > '2019-03-26 00:00:00'
      )
      or (
        pages.context_campaign_term in ('philo-1893', 'philo-1894', 'philo-1895', 'philo-1896')
        or pages.context_campaign_content in ('blackmusicmonthsavethedate', 'betawards', '2019betawards')
        or pages.path in ('/lala', '/kendall')
      )
      then 'try' 
  end as visit_type
from pages
