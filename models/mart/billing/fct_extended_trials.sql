{{ config(materialized="table", dist="account_id", sort="trial_start") }}

 SELECT * FROM {{ ref('rails_prod_extended_trials_stage') }}
 