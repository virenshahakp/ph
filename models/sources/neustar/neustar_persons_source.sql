with

persons as (

  select * from {{ source('demographics', 'neustar_persons') }}


)

, subset as (

  select
    user_id     as user_id
    , gender    as gender
    , education as education
    , index     as index -- noqa: RF04
    , coalesce(
      nullif(birth_year, 0)
      , date_part(year, last_updated_at::timestamp) - age + 1
    )           as birth_year -- major fill rate increase, adding 1 to age increases accuracy
  from persons

)

select * from subset
