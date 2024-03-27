with

subscribed as (

  select * from {{ ref('rails_prod_subscription_started_stage') }}

)

, paid as (

  select distinct account_id
  from {{ ref('rails_prod_payment_succeeded_stage') }}

)

, accounts_signed_up as (

  select * from {{ ref('fct_account_signed_up_all_sources') }}

)

, visit_all_sources as (

  select * from {{ ref('fct_visits_from_all_sources') }}

)

, accounts_signed_up_subset as (

  -- use account signed up all sources because signed up all sources contains profiles, which should
  -- be excluded from the funnel
  select *
  from accounts_signed_up
  where
    account_id not in (
      select account_id
      from
        paid
    )
    and account_id not in (
      select account_id
      from
        subscribed
    )

)

select
  accounts_signed_up_subset.account_id                                                        as account_id
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
  , visit_all_sources.platform                                                                as visit_platform
  , visit_all_sources.platform_type                                                           as visit_platform_type
  , visit_all_sources.platform_os                                                             as visit_platform_os
  , visit_all_sources.browser                                                                 as visit_browser
  , accounts_signed_up_subset.platform                                                        as signup_platform
  , visit_all_sources.url
  , coalesce(visit_all_sources.visited_at, accounts_signed_up_subset.signed_up_at)::timestamp as visited_at
  , accounts_signed_up_subset.signed_up_at::timestamp                                         as signed_up_at
  , null::timestamp                                                                           as subscribed_at
  , null::timestamp                                                                           as first_paid_at
  , visit_all_sources.anonymous_id                                                            as visit_anonymous_id
  , null                                                                                      as subscriber_billing
  , visit_all_sources.coupon_code                                                             as coupon_code
  , visit_all_sources.reference                                                               as reference
  , coalesce(
    visit_all_sources.visit_type
    , 'unknown'
  )                                                                                           as visit_type
  , coalesce(
    visit_all_sources.traffic_type
    , 'organic'
  )                                                                                           as traffic_type
  , row_number() over (
    partition by accounts_signed_up_subset.account_id
    order by
      visit_all_sources.priority asc nulls last
      , visit_all_sources.visited_at desc nulls last
  )                                                                                           as retarget
from accounts_signed_up_subset
left join visit_all_sources on (
  accounts_signed_up_subset.anonymous_id = visit_all_sources.anonymous_id
  and visit_all_sources.visited_at <= accounts_signed_up_subset.signed_up_at
)
