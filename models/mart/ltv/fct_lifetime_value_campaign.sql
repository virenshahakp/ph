{{ config(materialized="table", dist="all", sort="signup_month", tags=["daily", "exclude_hourly"], enabled=false) }}
WITH funnel AS (

  SELECT * FROM {{ ref('fct_acquisition_funnel') }}

)

, ltv AS ( 

  SELECT * FROM {{ ref('fct_lifetime_value') }}

)

SELECT
  DATE_TRUNC('month', funnel.signed_up_at)::DATE AS signup_month
  , CASE WHEN funnel.context_campaign_source IS NULL 
    THEN 'organic' 
    ELSE LOWER(funnel.context_campaign_source) END AS campaign_source
  , CASE WHEN funnel.context_campaign_name IS NULL 
    THEN 'organic' 
    ELSE LOWER(funnel.context_campaign_name) END AS campaign_name
  , CASE WHEN funnel.context_campaign_term IS NULL 
    THEN 'organic' 
    ELSE LOWER(funnel.context_campaign_term) END AS campaign_term   
  , AVG(ltv.ltv_revenue) AS avg_ltv_revenue
  , AVG(ltv.ltv_margin) AS avg_ltv_margin
  , AVG(ltv.months_with_payments) AS avg_ltv_months
FROM ltv 
LEFT JOIN funnel ON ltv.account_id = funnel.account_id
{{ dbt_utils.group_by(n=4) }}
ORDER BY signup_month