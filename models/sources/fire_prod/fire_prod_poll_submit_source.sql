with

poll_submit as (

  select * from {{ source('fire_prod', 'poll_submit') }}

)

, renamed as (

  select
    id                                                                               as event_id
    , user_id
    , name                                                                           as poll_name
    , question
    , answer
    , received_at
    , uuid_ts                                                                        as loaded_at
    , coalesce(environment_analytics_version, context_environment_analytics_version) as environment_analytics_version
  from poll_submit
  where user_id is not null

)

select * from renamed