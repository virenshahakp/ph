with

all_platforms as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_identifies_stage')
        , ref('androidtv_prod_identifies_stage')
        , ref('chromecast_prod_identifies_stage')
        , ref('fire_prod_identifies_stage')
        , ref('fire_tv_prod_identifies_stage')
        , ref('ios_prod_identifies_stage')
        , ref('roku_prod_identifies_stage')
        , ref('samsung_prod_identifies_stage')
        , ref('tvos_prod_identifies_stage')
        , ref('viziotv_prod_identifies_stage')
        , ref('web_prod_identifies_stage')
      ]
      , include=[
        "user_id"
        , "anonymous_id"
        , "received_at"
        , "timestamp"
        , "loaded_at"
      ]
    )
  }}

)

/*
  LB 3/2/2020: ios & tvos had a signup bug where anonymous ids
    that created an account would not be identified
    with the created account.
    This apple fix model recreates those identify calls.
*/

, apple_fix as (

  select
    anonymous_id
    , user_id
    , platform
    , "timestamp"
    , loaded_at
  from {{ ref('apple_signups_fix_2020_03_02') }}

)

select
  anonymous_id
  , user_id
  , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  , "timestamp"
  , loaded_at
from all_platforms

union all

select
  anonymous_id
  , user_id
  , platform
  , "timestamp"
  , loaded_at
from apple_fix