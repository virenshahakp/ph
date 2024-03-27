{{
  config(
    materialized='view'
  )
}}

with

user_experiments as (

  {{ 
    dbt_utils.union_relations(
      relations=[
        ref('web_prod_experiment_users_stage')
        , ref('rails_prod_experiment_users_stage')
      ]
    )
  }}

)

/*
for user signups, since someone can signup multiple times on different
platforms, but eventually connect those anonymous ids to a single account
we generate the row number so that we can identify the first event later
in the model
*/
, signups as (

  select
    anonymous_id
    , user_id
    , signed_up_at
    , row_number() over (partition by anonymous_id order by signed_up_at) as rn
  from {{ ref('fct_user_signed_up_all_sources') }}

)

, acquisition_funnel as (

  select * from {{ ref('fct_acquisition_funnel') }}

)

, viewed as (

  select
    anonymous_id      as anonymous_id
    , user_id         as user_id
    , experiment_name as experiment_id
    , experiment_name as experiment_name
    , variant         as variation_id
    , variant         as variation_name
    , event_timestamp as event_timestamp
  from user_experiments

)


, totals as (

  select
    viewed.anonymous_id
    , viewed.experiment_id
    , viewed.experiment_name
    , viewed.variation_id
    , viewed.variation_name
    , signups.user_id
    , viewed.event_timestamp
    , coalesce(accounts.visit_platform, visits_only.visit_platform)                   as visit_platform
    , coalesce(accounts.visit_platform_type, visits_only.visit_platform_type)         as visit_platform_type
    , coalesce(accounts.visit_platform_os, visits_only.visit_platform_os)             as visit_platform_os
    , coalesce(accounts.visit_browser, visits_only.visit_browser)                     as visit_browser
    , coalesce(accounts.signup_platform, visits_only.signup_platform)                 as signup_platform
    , coalesce(accounts.subscriber_billing, visits_only.subscriber_billing)           as subscriber_billing
    , coalesce(accounts.context_campaign_source, visits_only.context_campaign_source) as context_campaign_source
    , coalesce(accounts.context_campaign_name, visits_only.context_campaign_name)     as context_campaign_name
    , coalesce(accounts.context_page_path, visits_only.context_page_path)             as context_page_path
    , case
      when accounts.signed_up_at is null then 'New User'
      when accounts.signed_up_at > viewed.event_timestamp then 'New User'
      when accounts.signed_up_at < viewed.event_timestamp then 'Existing User'
      else 'Unknown User State'
    end                                                                               as new_or_existing
  from viewed
  left join signups
    on (
      viewed.anonymous_id = signups.anonymous_id
      and signups.rn = 1
    )
  left join acquisition_funnel as accounts on (signups.user_id = accounts.account_id)
  left join acquisition_funnel as visits_only on (viewed.anonymous_id = visits_only.visit_anonymous_id)

)

select * from totals
