with

pages as (

  select * from {{ ref('chromecast_prod_pages_source') }}

)

select * from pages
