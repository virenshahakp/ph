
WITH

payment_provider_purchase_success AS (

  SELECT * FROM {{ ref('ios_prod_payment_provider_purchase_success_source') }}

)

SELECT * FROM payment_provider_purchase_success
