
WITH

payment_provider_purchase_start AS (

  SELECT * FROM {{ ref('tvos_prod_payment_provider_purchase_start_source') }}

)

SELECT * FROM payment_provider_purchase_start
