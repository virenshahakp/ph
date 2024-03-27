{{
  config(
    materialized='table'
    , sort='visited_at'
    , dist='anonymous_id'
  )
}}

with

attributed_visits as (

  select * from {{ ref('all_platforms_attributed_visits') }}

)

/*
  TODO: move this to a doc block

  Visits are entry points into our product. For example, ad clicks that lead
  users to try.philo.com as well as organic searches and direct visits to
  try.philo.com. We also track ad clicks and app launches on 10-ft platforms
  like Roku, FireTV, and tvOS. Link sharing an referrals are also visit points.

  We assign a notion of priority to visits. Visits from ad clicks take higher
  priority than organic clicks (which are are not from ads). This view tracks all
  visits for each user. The consumer of this view must filter visits and must
  also take priority into account if they are trying to find a particular visit.
  Here is the current priority hierarchy:

  priority 1:
    - any paid visits
    - referral
  priority 2:
    - we want to attribute it to the original platform when users have to finish
      registration on try.philo.com
    - shared links
  priority 3:
    - all else
*/

select
  {{ dbt_utils.star(from=ref('all_platforms_attributed_visits'), except=["user_id"]) }}
from attributed_visits
-- exclude items for which a priority and visit type has not been set
-- only look at events with no user_id
where priority is not null
  and visit_type is not null
  and user_id is null
