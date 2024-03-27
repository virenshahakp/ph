with

watched_range as (

  select * from {{ ref('derived_androidtv_prod_watched_range_source') }}

)

select * from watched_range
