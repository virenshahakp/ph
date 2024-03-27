WITH

source AS (

  SELECT * FROM {{ ref('fire_prod_identifies_source') }}

)

SELECT * FROM source
