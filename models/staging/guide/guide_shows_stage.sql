{{ config(materialized='ephemeral') }}
with 

shows as (

  select * from {{ ref('guide_shows_source') }}

)

, remove_duplicates as (

  /*
  There are duplicates in the source data which we need to clean up. For now,
  de-duplicate here.
  */
  select distinct * from shows

)

select
  *
  , coalesce(program_type, '') in ('TBA', 'Paid Programming', 'Off Air') as is_paid_programming
from remove_duplicates