with

source as (

  select * from {{ ref('androidtv_prod_identifies_source') }}

)

select * from source
