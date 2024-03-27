with

agents as (

  select
    zd_user_id                            as agent_id
    , philo_user_id                       as user_id
    , has_phone
    , has_email
    , organization_id
    , agent_role
    , custom_role_id
    , default_group_id
    , tags
    , suspended                           as is_suspended
    , created_at
    , updated_at
    , email
    , vendor
    , coalesce(nullif(site, ''), 'Other') as site
  from {{ source('zendesk', 'zd_agents') }}

)

select * from agents