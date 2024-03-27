with 

signup_code_generated as (

  select * from {{ ref('rails_prod_signup_code_generated_source') }}

)

select * from signup_code_generated
