with

watched_range as (

  select * from {{ ref('derived_tvos_prod_watched_range_source') }}

)

select * from watched_range
