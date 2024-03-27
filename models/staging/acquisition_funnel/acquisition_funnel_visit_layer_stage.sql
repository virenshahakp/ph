with

signedup as (

  select * from {{ ref('fct_user_signed_up_all_sources') }}

)

, visit_all_sources as (

  select * from {{ ref('fct_visits_from_all_sources') }}

)

select
  visit_all_sources.anonymous_id            as account_id
  , visit_all_sources.context_ip
  , visit_all_sources.context_campaign_source
  , visit_all_sources.context_campaign_name
  , visit_all_sources.context_campaign_term
  , visit_all_sources.context_campaign_medium
  , visit_all_sources.context_page_referrer
  , visit_all_sources.context_user_agent
  , visit_all_sources.context_page_path
  , visit_all_sources.context_campaign_content
  , visit_all_sources.context_campaign_content_id
  , visit_all_sources.platform              as visit_platform
  , visit_all_sources.platform_type         as visit_platform_type
  , visit_all_sources.platform_os           as visit_platform_os
  , visit_all_sources.browser               as visit_browser
  , null                                    as signup_platform
  , visit_all_sources.url
  , visit_all_sources.visited_at::timestamp as visited_at
  , null::timestamp                         as signed_up_at
  , null::timestamp                         as subscribed_at
  , null::timestamp                         as first_paid_at
  , visit_all_sources.anonymous_id          as visit_anonymous_id
  , null                                    as subscriber_billing

  , visit_all_sources.coupon_code
  , visit_all_sources.reference
  , coalesce(
    visit_all_sources.visit_type
    , 'unknown'
  )                                         as visit_type
  , coalesce(
    visit_all_sources.traffic_type
    , 'organic'
  )                                         as traffic_type
  , row_number() over (
    partition by visit_all_sources.anonymous_id
    order by
      priority asc nulls last
      , visit_all_sources.visited_at desc nulls last
  )                                         as retarget
from visit_all_sources
where
  visit_all_sources.anonymous_id not in (
    select anonymous_id
    from
      signedup -- still use signed up all sources here, so we can exclude profiles switches from the visits.
    where
      anonymous_id is not null
  )
