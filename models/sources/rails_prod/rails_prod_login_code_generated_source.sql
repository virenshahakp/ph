with

login_code_generated as (

  select * from {{ source('rails_prod', 'login_code_generated') }}

)

, renamed as (

  select
    id            as event_id
    , received_at
    , event
    , hashed_session_id
    , anonymous_id
    , context_ip  as client_ip
    , context_page_referrer
    , context_page_url
    , environment_analytics_version
    , environment_namespace
    , path
    , "timestamp" as event_timestamp
    , context_page_host
    , event_text
    , host
    , referrer
    , user_id
    , url
    , uuid_ts     as loaded_at
    , context_active
    , context_library_version
    , context_page_path
    , context_user_agent
    , sent_at
    , context_library_name
    , original_timestamp
    , query
    , context_page_search
  from login_code_generated

)

select * from renamed