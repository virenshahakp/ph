{{ config(materialized='view') }}

select *
from {{ ref('ungranted_locks_stage') }}