with

appsflyer as (

  select * from {{ ref('appsflyer_amazon_installs_source') }}

)

select * from appsflyer