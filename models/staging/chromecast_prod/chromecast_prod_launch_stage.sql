with

launches as (

  select * from {{ ref('chromecast_prod_launch_source') }}

)

select * from launches
