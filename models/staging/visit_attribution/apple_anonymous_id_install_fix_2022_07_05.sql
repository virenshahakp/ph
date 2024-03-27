{{ config(materialized='ephemeral') }}

/*
This view is not materialized, because it is only used in signed_up_all_sources which materializes the output we use.


Apple devices had a bug where the anonymous_id was reset after the application_install event was
fired and the first philo screens were displayed.  This caused us to count the installs, but lose
the attribution for the user signing up.  We can correct this by joining on the context_device_id
which is the same.
*/

with

ios_installs as (

  select * from {{ ref('ios_prod_application_installed_source') }}

)

, ios_screens as (

  select * from {{ ref('ios_prod_screens_source') }}

)

, tvos_installs as (

  select * from {{ ref('tvos_prod_application_opened_source') }}

)

, tvos_screens as (

  select * from {{ ref('tvos_prod_screens_source') }}

)

, ios_installs_combined as (

  select
    ios_installs.event_id
    , ios_screens.anonymous_id
    , 'ios'                        as platform
    , min(ios_installs.visited_at) as "timestamp"
  from ios_installs
  join ios_screens
    on (
      ios_installs.context_device_id = ios_screens.context_device_id
      and ios_installs.anonymous_id != ios_screens.anonymous_id
      and {{ dbt.datediff(
            'ios_installs.visited_at'
            , 'ios_screens.visited_at'
            , 'minute')
        }} between 0 and 1
    )
  where
    ios_installs.context_app_version
    in (
      '3.4.0', '3.4.2', '3.4.3', '3.4.4', '3.4.5', '3.4.6', '3.4.7', '3.4.8'
      , '3.5.0', '3.5.1', '3.5.2'
      , '3.6.0', '3.6.1', '3.6.2'
      , '3.7.0', '3.7.2', '3.7.3'
    )
  {{ dbt_utils.group_by(n=3) }}

)

, tvos_installs_combined as (

  select
    tvos_installs.event_id
    , tvos_screens.anonymous_id
    , 'tvos'                        as platform
    , min(tvos_installs.visited_at) as "timestamp"
  from tvos_installs
  join tvos_screens
    on (
      tvos_installs.context_device_id = tvos_screens.context_device_id
      and tvos_installs.anonymous_id != tvos_screens.anonymous_id
      and {{ dbt.datediff(
            'tvos_installs.visited_at'
            , 'tvos_screens.visited_at'
            , 'minute')
        }} between 0 and 1
    )
  where
    tvos_installs.context_app_version
    in (
      '3.4.0', '3.4.2', '3.4.3', '3.4.4', '3.4.5', '3.4.6', '3.4.7', '3.4.8'
      , '3.5.0', '3.5.1', '3.5.2'
      , '3.6.0', '3.6.1', '3.6.2'
      , '3.7.0', '3.7.2', '3.7.3'
    )
  {{ dbt_utils.group_by(n=3) }}

)

select
  event_id
  , anonymous_id
  , platform
  , "timestamp"
from ios_installs_combined

union -- noqa: L033

select
  event_id
  , anonymous_id
  , platform
  , "timestamp"
from tvos_installs_combined
