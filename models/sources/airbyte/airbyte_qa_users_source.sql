with

qa as (

  select * from {{ source('airbyte', 'qa_users') }}

)

, renamed as (

  select
    "user"            as user_email
    , "user phone"    as user_phone
    , "user role"     as user_role
    , pg              as product_generation
    , "billing state" as billing_state
    , "billed by"     as billed_by
    , attribute       as attribute
    , package         as package
    , uid             as user_rails_id
    , uuid            as external_uuid
    , notes           as notes
    , "last updated"  as last_updated_at
    , "last verified" as last_verified_at
  from qa

)

select * from renamed
