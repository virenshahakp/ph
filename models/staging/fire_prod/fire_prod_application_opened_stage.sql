WITH 

opened AS (

  SELECT * FROM {{ ref('fire_prod_application_opened_source') }}

)

SELECT * FROM opened
