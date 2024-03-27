{{ config(
    materialized = 'table'
    , sort = 'last_updated_at'
    , dist = 'ALL'
) }}


SELECT
  user_email
  , user_phone
  , user_role
  , product_generation
  , billing_state
  , billed_by
  , attribute
  , package
  , user_rails_id
  , external_uuid
  , notes
  , last_updated_at
  , last_verified_at
FROM {{ ref('airbyte_qa_users_stage') }}
