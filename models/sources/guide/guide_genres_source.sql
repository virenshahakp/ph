with

genre as (

  select * from {{ source('guide','genres') }}

)

, renamed as (

  select
    id     as genre_id
    , name as genre_name
  from genre

)

select * from renamed