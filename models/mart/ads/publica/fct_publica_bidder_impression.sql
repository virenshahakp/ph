{{ config(materialized='view') }}

select * from {{ ref('publica_bidder_impression_dyn_source') }}