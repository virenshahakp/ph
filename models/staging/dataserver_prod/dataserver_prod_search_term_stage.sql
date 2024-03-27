-- ignore L014 because there is something buggy with sqlfluff and this file
--noqa: disable=L014
with

searches as (

  select
    user_id
    , term
    , user_agent
    , received_at
    , search_at
  from {{ ref('dataserver_prod_search_term_source') }}

)

, grouped_queries as (

  select
    *
    , case
      when lead(term) over (partition by user_id order by search_at) is null
        then term
      when lead(term) over (partition by user_id order by search_at) like term + '%'
        then null
      else term
    end as query
  from searches
  order by search_at

)

select
  *
  , row_number() over (partition by user_id order by received_at asc) as search_number
from grouped_queries
where query is not null

