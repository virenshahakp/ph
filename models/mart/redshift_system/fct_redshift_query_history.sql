{{ config(materialized='view') }}

select *
from {{ ref('query_history_stage') }}
