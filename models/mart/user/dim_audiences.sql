{{ config(materialized='table', sort=['audience_name','audience'], dist='audience_name' ) }}

WITH

audiences AS (

  SELECT * FROM {{ ref('fct_account_audiences') }}

)

SELECT 
  audience
  , audience_name
  , COUNT(account_id) AS audience_size
FROM audiences
WHERE is_billable IS TRUE
{{ dbt_utils.group_by(n=2) }}