with

opened as (

  select * from {{ ref('androidtv_prod_application_opened_source') }}

)

select * from opened
