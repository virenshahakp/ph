{{ config(materialized='view') }}

select * from {{ ref('publica_bid_requested_dyn_source') }}