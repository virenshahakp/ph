WITH 

source AS (

  SELECT * FROM {{ ref('ios_prod_identifies_source') }}

)

SELECT * FROM source
