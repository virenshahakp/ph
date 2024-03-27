with 

installed as (

  select * from {{ ref('samsung_prod_application_opened_source') }}

)

select * from installed
