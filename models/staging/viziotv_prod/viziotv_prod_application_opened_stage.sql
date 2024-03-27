with

installed as (

  select * from {{ ref('viziotv_prod_application_opened_source') }}

)

select * from installed
