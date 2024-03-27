with

users as (

  select * from {{ ref('rails_prod_users_source') }}

)

select
  user_id
  , account_id
  , is_root as is_account_owner
  , roles
  , subscriber_billing
  , subscriber_state
  , created_at
  , labels
  , packages
  , zip
  , dma_code
  , dma_region
  , dma_name
  , income
  , age_range
  , gender
  , referrer_id
  , referral_type
  , has_email
  , has_phone
  , is_direct_billed
  , signup_source
  , loaded_at
from users
