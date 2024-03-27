{{ config(materialized='table', sort='audience_name', dist='account_id' ) }}

WITH

billable_accounts AS (

  SELECT * FROM {{ ref('dim_accounts') }}

)

, all_audiences AS (

  SELECT * FROM {{ ref('all_account_audiences') }}

)

, acquisition_funnel AS (

  SELECT * FROM {{ ref('fct_acquisition_funnel') }}

)

SELECT 
  all_audiences.account_id
  , all_audiences.audience
  , all_audiences.audience_name
  , acquisition_funnel.visited_at 
  , acquisition_funnel.signed_up_at  IS NOT NULL   AS did_sign_up
  , acquisition_funnel.subscribed_at IS NOT NULL   AS did_subscribe
  , acquisition_funnel.first_paid_at IS NOT NULL   AS did_pay
  , COALESCE(billable_accounts.is_billable, FALSE) AS is_billable
FROM all_audiences
LEFT JOIN acquisition_funnel ON (all_audiences.account_id = acquisition_funnel.account_id)
LEFT JOIN billable_accounts ON (all_audiences.account_id = billable_accounts.account_id)

