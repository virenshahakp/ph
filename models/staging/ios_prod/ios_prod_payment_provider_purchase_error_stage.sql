
WITH

payment_provider_purchase_error AS (

  SELECT * FROM {{ ref('ios_prod_payment_provider_purchase_error_source') }}

)

SELECT * FROM payment_provider_purchase_error
