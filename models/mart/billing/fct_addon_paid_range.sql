{{ config(materialized="table", dist="account_id", sort="date_range_start") }}

 SELECT * FROM {{ ref('rails_prod_addon_paid_range_stage') }}
 
