{{ config(materialized='table', sort='show_id', dist='ALL') }}

select * from {{ ref('guide_shows_stage') }}
