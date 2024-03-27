WITH

source AS (

  SELECT * FROM {{ ref('tvos_prod_signup_auth_submit_source') }}

)

SELECT * FROM source
