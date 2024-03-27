WITH 

opened AS (

    SELECT * FROM {{ ref('tvos_prod_application_opened_source') }}

)

SELECT * FROM opened
