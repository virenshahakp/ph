{{
  config(
    materialized = 'view',
  )
}}

select * from {{ ref('export_experiments_stage') }}