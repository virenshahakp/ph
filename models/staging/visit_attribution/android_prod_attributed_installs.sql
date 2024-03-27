WITH 

app_installs AS (

  SELECT * FROM {{ ref('android_prod_application_installed_stage')}}

)

SELECT 
  *
FROM app_installs 
WHERE COALESCE(TRIM(attributed_touch_type), '') != ''
