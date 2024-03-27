{{ config(materialized='view') }}
WITH

genre AS (

  SELECT * FROM {{ ref('genres_stage')}}

)

, show AS (

  SELECT * FROM {{ ref('genres_shows_stage')}} 
  
)

SELECT

  genre.genre_name
  , show.genre_id
  , show.show_id
  , show.created_at
  , show.updated_at
FROM show
LEFT JOIN genre ON (show.genre_id = genre.genre_id)