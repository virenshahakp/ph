with

identifies as (

  select * from {{ ref('bby_billing_api_prod_identifies_source') }}

)

select * from identifies
