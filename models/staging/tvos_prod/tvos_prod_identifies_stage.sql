WITH 

source AS (

    SELECT * FROM {{ ref('tvos_prod_identifies_source') }}

)

SELECT * FROM source
