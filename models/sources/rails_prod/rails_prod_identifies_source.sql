with

identifies as (

  select * from {{ source('rails_prod', 'identifies') }}

)

select
  user_id
  , root_user_id as account_id
  , anonymous_id
  , received_at

  , hashed_session_id
  , subscriber_state
  , subscriber_billing
  , roles
  , packages

  , zip
  , age_range
  , dma_region
  , gender
  , dma_code
  , dma_name
  , income
  , has_phone
  , has_email
from identifies
