with

poll_submit as (

  select * from {{ source('tvos_prod', 'poll_submit') }}

)

, renamed as (

  select
    id        as event_id
    , user_id
    , name    as poll_name
    , question
    , answer
    , environment_analytics_version
    , received_at
    , uuid_ts as loaded_at
  from poll_submit
  where user_id is not null

)

select * from renamed