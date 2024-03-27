with

modified as (

  select * from {{ ref('bby_billing_api_prod_modified_source') }}

)

, bestbuy_identifies as (

  select * from {{ ref('bby_billing_api_prod_identifies_stage') }}

)

, subscription_modified as (

  select
    modified.*
    , bestbuy_identifies.account_id as account_id
  from modified
  join bestbuy_identifies on (modified.anonymous_id = bestbuy_identifies.anonymous_id)
  where modified.modify_reason_code is not null

)

select * from subscription_modified
