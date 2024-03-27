with

genres_shows as (

  select * from {{ source('guide','genres_shows') }}

)

, renamed as (

  select
    genre_id
    , show_id
    , created_at
    , updated_at
  from genres_shows

)

select * from renamed