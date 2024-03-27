with

all_identifies as (

  select *
  from {{ ref('fct_user_identifies') }}
  where user_id is not null

)

, rails_account_created as (

  select
    user_id
    , event_timestamp
    , signup_source
  -- EM: for FAST Phase 1.5, excluding FAST-only account created events. Will revisit in next phase of FAST.
  from {{ ref('rails_prod_account_created_paid_stage') }}
  where user_id is not null

)

, web_prod_identifies as (

  select
    *
    , case when context_user_agent ~* ('iphone|ipad')
        then 'ios-'
      when context_user_agent ilike '%android%'
        then 'android-'
      else ''
    end || 'web' as platform
  from {{ ref('web_prod_identifies_stage') }}
  where user_id is not null

)

, all_identifies_web_user_agent as (

  select
    all_identifies.anonymous_id
    , all_identifies.user_id
    , all_identifies."timestamp"
    , case when all_identifies.platform = 'web'
        then coalesce(web_prod_identifies.platform, all_identifies.platform)
      else all_identifies.platform
    end as platform
  from all_identifies
  left join web_prod_identifies on (
    all_identifies.anonymous_id = web_prod_identifies.anonymous_id
    and all_identifies."timestamp" = web_prod_identifies."timestamp"
  )

)

, all_identifies_and_accounts as (

  select
    null  as anonymous_id
    , user_id
    , case
      when signup_source = 'roku_isu' then 'roku'
      else 'rails'
    end   as platform
    , event_timestamp as "timestamp"
    -- make any rails timestamp a future date
    -- this allows it to sort last and be the last possible
    -- signup platform identified
    , {{ dbt.dateadd(datepart='day', interval=10, from_date_or_timestamp='current_date') }} as platform_timestamp
  from rails_account_created

  union all

  select
    anonymous_id
    , user_id
    , platform
    , "timestamp"
    , "timestamp" as platform_timestamp
  from all_identifies_web_user_agent

)

select distinct
  anonymous_id
  , user_id
  , min("timestamp") over (partition by user_id) as "timestamp"
  , first_value(platform ignore nulls) over (
    partition by user_id
    order by platform_timestamp
    rows between unbounded preceding and unbounded following
  )                                              as platform
from all_identifies_and_accounts
