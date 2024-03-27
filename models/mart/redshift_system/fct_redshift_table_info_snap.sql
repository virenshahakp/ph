{{ config(materialized='view') }}

select *
from {{ ref('svv_table_info_stage') }}