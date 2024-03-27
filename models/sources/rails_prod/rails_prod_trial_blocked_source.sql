with

trial_blocked as (

  select * from {{ source('rails_prod', 'trial_blocked') }}

)

, renamed as (

  select
    id        as trial_blocked_id
    , user_id as account_id

    -- event timestamps
    , received_at
    , original_timestamp
    , "timestamp"
    , uuid_ts
    , sent_at

    -- segment context
    , event
    , event_text
    , context_environment_analytics_version
    , context_environment_billing_cluster
    , context_environment_namespace
    , context_rails_env
    , context_rails_version
    , context_library_name
    , context_library_version
    , context_environment_billing_context
    , context_environment_billing_mode
    , context_active
    , environment_namespace
    , environment_analytics_version
  from trial_blocked

)

select * from renamed


