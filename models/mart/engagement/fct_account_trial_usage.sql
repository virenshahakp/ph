{{ config(materialized='view') }}
WITH

historic_and_recent AS (

  {{ dbt_utils.union_relations(
    relations=[
        ref('watched_minutes_in_trial_historic')
      , ref('watched_minutes_in_trial_recent')
    ],
    include=[
        "account_id"
      , "created_at"
      , "profiles_used"
      , "minutes"
      , "sessions"
      , "distinct_shows"
      , "distinct_platforms"
      , "active_days"
    ]
  ) }}

)


SELECT 
  historic_and_recent.*
FROM historic_and_recent
