with

signup_code_generated as (

  select * from {{ source('rails_prod', 'signup_code_generated') }}

)

, renamed as (

  select
    id            as event_id
    , received_at
    , url
    , context_library_name
    , context_page_host
    , event_text
    , hashed_session_id
    , original_timestamp
    , environment_namespace
    , event
    , host
    , user_id
    , environment_analytics_version
    , anonymous_id
    , context_ip
    , context_page_path
    , context_page_referrer
    , context_page_url
    , referrer
    , "timestamp" as event_timestamp
    , uuid_ts     as loaded_at
    , context_active
    , context_library_version
    , context_user_agent
    , path
    , sent_at
    , context_page_search
    , query
  from signup_code_generated

)

select * from renamed