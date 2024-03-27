with

payment_failed as (

  select * from {{ source('rails_prod', 'payment_failed') }}

)

, renamed as (

  select
    id                                          as payment_failed_id
    , user_id                                   as account_id
    , amount                                    as amount
    , subscriber_state                          as subscriber_state
    , rev_share_partner                         as rev_share_partner
    , received_at

    -- event timestamps
    , sent_at
    , "timestamp"
    , original_timestamp
    , event

    -- segment context
    , event_text
    , context_environment_billing_cluster
    , context_environment_billing_mode
    , context_rails_env
    , context_environment_billing_context
    , context_library_version
    , context_environment_namespace
    , context_library_name
    , context_environment_analytics_version
    , context_rails_version
    , context_active
    , environment_analytics_version
    , environment_namespace
    , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
  from payment_failed

)

select * from renamed


