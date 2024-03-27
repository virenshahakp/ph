WITH 

app_installs AS (

  SELECT * FROM {{ ref('ios_prod_application_installed_stage')}}

)

SELECT 
  *
FROM app_installs 
WHERE COALESCE(TRIM(attributed_touch_type), '') != ''
