with

trial_started as (

  select * from {{ source('rails_prod', 'trial_started') }}

)

, renamed as (

  select
    id        as trial_started_id
    , user_id as account_id

    -- free_trial is the time prior to subscribe
    -- overall_trial is the full period before first payment
    , free_trial_duration
    , overall_trial_duration
    , trial_started_at
    , free_trial_ends_at
    , overall_trial_ends_at

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
  from trial_started

)

select * from renamed

