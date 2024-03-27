--EM: OK as a view for now, may want to update to incremental table in future
with

credentialed as (

  select * from {{ ref('rails_prod_user_credentialed_source') }}

)

select * from credentialed
