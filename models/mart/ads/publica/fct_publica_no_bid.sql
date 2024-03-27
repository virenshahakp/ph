{{ config(materialized='view') }}

select * from {{ ref('publica_no_bid_dyn_source') }}