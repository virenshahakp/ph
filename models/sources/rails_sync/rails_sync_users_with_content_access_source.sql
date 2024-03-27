with

source as (

  select * from {{ source('rails_sync', 'users_with_content_access') }}

)

, renamed as (

  select
    user_id  as account_id
    , biller as subscriber_billing
    , status
    , product_sku
    , activated_plan
    , reported_at
  from source

)

select * from renamed