{{
  config(
    materialized = 'view',
    )
}}

select * from {{ ref('rails_prod_package_payment_succeeded_stage') }}