with

subscribed as (

  select * from {{ ref('rails_prod_subscription_started_stage') }}

)

, paid as (

  select * from {{ ref('rails_prod_payment_succeeded_stage') }}

)

, accounts_signed_up as (

  select * from {{ ref('fct_account_signed_up_all_sources') }}

)

, visit_all_sources as (

  select * from {{ ref('fct_visits_from_all_sources') }}

)

select
  paid.account_id                   as account_id
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
  , visit_all_sources.platform      as visit_platform
  , visit_all_sources.platform_type as visit_platform_type
  , visit_all_sources.platform_os   as visit_platform_os
  , visit_all_sources.browser       as visit_browser
  , accounts_signed_up.platform     as signup_platform
  , visit_all_sources.url
  , coalesce(
    visit_all_sources.visited_at
    , accounts_signed_up.signed_up_at
    , subscribed.subscribed_at
    , first_paid_at
  )::timestamp                      as visited_at
  , coalesce(
    accounts_signed_up.signed_up_at
    , subscribed.subscribed_at
    , paid.first_paid_at
  )::timestamp                      as signed_up_at
  , coalesce(
    subscribed.subscribed_at
    , first_paid_at
  )::timestamp                      as subscribed_at
  , paid.first_paid_at::timestamp   as first_paid_at
  , visit_all_sources.anonymous_id  as visit_anonymous_id
  , subscribed.subscriber_billing   as subscriber_billing
  , visit_all_sources.coupon_code   as coupon_code
  , visit_all_sources.reference     as reference
  , coalesce(
    visit_all_sources.visit_type
    , 'unknown'
  )                                 as visit_type
  , coalesce(
    visit_all_sources.traffic_type
    , 'organic'
  )                                 as traffic_type
  , row_number() over (
    partition by paid.account_id
    order by
      visit_all_sources.priority asc nulls last
      , visit_all_sources.visited_at desc nulls last
  )                                 as retarget
from paid
left join subscribed
  on (
    paid.account_id = subscribed.account_id
    and subscribed.subscribed_at <= paid.first_paid_at
  )
left join accounts_signed_up
  on (
    paid.account_id = accounts_signed_up.account_id
    and accounts_signed_up.signed_up_at
    <= coalesce(
      subscribed.subscribed_at
      , paid.first_paid_at
    )
  )
left join visit_all_sources as visit_all_sources on (
  visit_all_sources.anonymous_id = accounts_signed_up.anonymous_id
  and visit_all_sources.visited_at <= accounts_signed_up.signed_up_at
)
