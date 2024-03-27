{{ config(materialized='view') }}

select *
from {{ ref('query_metrics_stage') }}
