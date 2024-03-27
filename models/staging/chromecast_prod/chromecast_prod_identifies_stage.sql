with

identifies as (

  select * from {{ ref('chromecast_prod_identifies_source') }}

)

select * from identifies
