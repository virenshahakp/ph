WITH

source AS (

  SELECT * FROM {{ ref('fire_tv_prod_identifies_source') }}

)

SELECT * FROM source
