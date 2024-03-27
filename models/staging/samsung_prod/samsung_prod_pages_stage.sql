with

pages as (

  select * from {{ ref('samsung_prod_pages_source') }}

)

select * from pages