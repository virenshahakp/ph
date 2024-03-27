{{ config(materialized="table", dist="account_id", sort="purchased_at") }}

 SELECT * FROM {{ ref('bby_billing_api_prod_events_stage') }}
 



