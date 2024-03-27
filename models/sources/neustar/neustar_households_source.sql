with

households as (

  select
    user_id         as user_id
    , homeowner     as homeowner
    , total_persons as people_in_household
    , composition   as composition
    , income_narrow as income_narrow
  from {{ source( 'demographics', 'neustar_households') }}

)

select * from households