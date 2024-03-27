WITH watched AS ( 

  SELECT * FROM {{ ref('fct_watched_minutes') }}

)

, show_genres AS ( 

  SELECT * FROM {{ ref('fct_guide_genre_shows') }}

)

, guide AS (

  SELECT * FROM {{ ref('dim_guide_metadata') }}

)

, users AS (

  SELECT * FROM {{ ref('dim_users') }}

)

, ltv AS ( 

  SELECT * FROM {{ ref('fct_lifetime_value') }} 

)

, asset_guide AS (

  SELECT 
    watched.*
    , users.account_id
    , show_genres.genre_id
    , show_genres.genre_name
  FROM watched
  LEFT JOIN guide ON (watched.asset_id = guide.asset_id)
  LEFT JOIN show_genres ON (watched.show_id = show_genres.show_id) 
  LEFT JOIN users ON (watched.user_id = users.user_id)
  WHERE guide.has_public_view IS TRUE

)

, genre_week AS (
  
  SELECT 
    asset_guide.genre_name
    , asset_guide.account_id
    , asset_guide.genre_id
    , DATE_TRUNC('month', asset_guide.received_at)::DATE AS watch_month
    , COUNT(asset_guide.show_id) AS shows
    , COUNT(asset_guide.episode_id) AS episodes
    , COUNT(DISTINCT asset_guide.user_id) AS unique_users
  FROM asset_guide
 {{ dbt_utils.group_by(n=4) }}
  
)

SELECT
  ltv.account_id
  , genre_week.watch_month
  , genre_week.genre_name
  , genre_week.genre_id
  , ltv.avg_ltv_revenue
  , ltv.months_with_payments
  , ltv.ltv_revenue
  , ltv.ltv_margin
FROM genre_week 
LEFT JOIN ltv ON (ltv.account_id = genre_week.account_id)
WHERE genre_week.genre_name IS NOT NULL