with

persons as (

  select * from {{ ref('neustar_persons_stage') }}

)

, count_people as (

  select
    user_id
    , sum(case when gender = 'M' then 1 else 0 end) as males
    , sum(case when gender = 'F' then 1 else 0 end) as females
  from persons
  group by 1

)

select * from count_people
