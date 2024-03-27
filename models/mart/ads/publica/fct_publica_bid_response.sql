{{ config(materialized='view') }}

select * from {{ ref('publica_bid_response_dyn_source') }}