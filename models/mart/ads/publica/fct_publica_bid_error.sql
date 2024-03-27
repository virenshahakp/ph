{{ config(materialized='view') }}

select * from {{ ref('publica_bid_error_dyn_source') }}