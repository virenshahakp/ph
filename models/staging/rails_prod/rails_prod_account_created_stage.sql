with 

created as (

  select * from {{ ref('rails_prod_account_created_source') }}

)

select * from created
