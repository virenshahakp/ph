{{ config(materialized="table", dist="account_id", sort="received_at") }}

 SELECT * FROM {{ ref('rails_prod_package_added_stage') }}