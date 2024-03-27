WITH 

opened AS (

  SELECT * FROM {{ ref('fire_tv_prod_screens_source') }}
  
)

SELECT *
FROM opened
-- select is the screen name for an application being opened
WHERE name = 'select'
