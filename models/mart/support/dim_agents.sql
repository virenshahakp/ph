{{ config(
    materialized='table'
    , dist='agent_id'
    , sort='updated_at'
    , tags=["daily", "exclude_hourly"]
   )
}}
with

agents as (

  select * from {{ ref('zendesk_zd_agents_stage') }}

)

select * from agents