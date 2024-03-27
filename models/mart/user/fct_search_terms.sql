{{ config(materialized='table', dist='user_id', sort=['search_number', 'query', 'received_at']) }}

WITH 

search_terms AS (

  SELECT * FROM {{ ref('dataserver_prod_search_term_stage') }}
  
)

SELECT 
  user_id
  , query
  , user_agent
  , search_number
  , received_at
FROM search_terms
