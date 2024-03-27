WITH  

watched_minutes AS (

  SELECT * FROM {{ ref('fct_watched_minutes') }} 

)

, ltv AS (
  
  SELECT * FROM {{ ref('fct_lifetime_value') }} 
)

, users AS (

  SELECT * FROM {{ ref('dim_users') }} 

)

, hours AS (

  SELECT 
    users.account_id
    , watched_minutes.asset_id
    , watched_minutes.show_id
    , watched_minutes.show_title
    , watched_minutes.channel_name
    , DATE_TRUNC('month', watched_minutes.received_at)::DATE AS watch_month-- noqa: L029
    , SUM(watched_minutes.minutes / 60) AS hours_per_user
  FROM watched_minutes
  LEFT JOIN users ON (users.user_id = watched_minutes.user_id)
  {{ dbt_utils.group_by (n=6) }}

)

SELECT
  hours.show_title
  , hours.show_id
  , hours.channel_name
  , hours.watch_month
  , hours.hours_per_user
  , COUNT(DISTINCT hours.account_id) AS number_of_users
  , AVG(ltv.months_with_payments) AS avg_billed_months
  , AVG(ltv.ltv_margin) AS ltv_margin
  , AVG(ltv.ltv_revenue) AS ltv_revenue
FROM ltv 
LEFT JOIN hours ON hours.account_id = ltv.account_id
{{ dbt_utils.group_by (n=5) }}
ORDER BY ltv_revenue DESC