with

products as (

  select
    id          as product_id
    , batch_timestamp
    , active    as is_active
    , caption
    , created   as product_created_at
    , deactivate_on
    , description
    , name      as product_name
    , shippable as is_shippable
    , statement_descriptor
    , type
    , unit_label
    , url
  from {{ source('stripe_prod', 'products') }}

)

select * from products