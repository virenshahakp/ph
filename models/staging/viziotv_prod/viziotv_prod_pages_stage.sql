with

pages as (

  select * from {{ ref('viziotv_prod_pages_source') }}

)

select * from pages
