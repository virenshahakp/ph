with

refunds as (

  select * from {{ source('rails_prod', 'payment_refunded') }}

)

, renamed as (

  select
    id                                          as payment_refunded_id
    , user_id                                   as account_id
    , amount                                    as amount
    , received_at

    -- event timestamps
    , sent_at
    , "timestamp"
    , original_timestamp
    , event

    -- segment context
    , event_text
    , context_rails_version
    , context_environment_billing_context
    , context_environment_billing_mode
    , context_library_name
    , context_environment_analytics_version
    , context_rails_env
    , context_environment_namespace
    , context_library_version
    , context_environment_billing_cluster
    , context_active
    , environment_analytics_version
    , environment_namespace
    , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
  from refunds

)

select * from renamed
