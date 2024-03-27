with

tv_spend as (

  select * from {{ ref('uploads_daily_tv_performance_stage') }}

)

select * from tv_spend