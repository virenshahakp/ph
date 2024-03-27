with

watched_range as (

  select * from {{ ref('derived_roku_prod_watched_range_source') }}

)

select * from watched_range
