WITH

source AS (

  SELECT * FROM {{ ref('ios_prod_signup_auth_success_source') }}

)

SELECT * FROM source
