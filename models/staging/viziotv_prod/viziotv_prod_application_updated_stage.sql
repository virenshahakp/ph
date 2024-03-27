with

updated as (

  select * from {{ ref('viziotv_prod_application_updated_source') }}

)

select * from updated
