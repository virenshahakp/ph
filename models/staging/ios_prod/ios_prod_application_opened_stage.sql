with 

opened as (

  select * from {{ ref('ios_prod_application_opened_source') }}

)

select * from opened