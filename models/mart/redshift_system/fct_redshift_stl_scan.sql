{{ config(materialized='view') }}

select *
from {{ ref('stl_scan_stage') }}
