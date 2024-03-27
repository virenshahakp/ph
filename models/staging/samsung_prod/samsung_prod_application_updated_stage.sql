with 

updated as (

  select * from {{ ref('samsung_prod_application_updated_source') }}

)

select * from updated
