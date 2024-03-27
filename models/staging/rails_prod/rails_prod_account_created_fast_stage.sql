with

created as (

  select * from {{ ref('rails_prod_account_created_stage') }}

)

select * from created
where signup_source = 'FAST 1.5'