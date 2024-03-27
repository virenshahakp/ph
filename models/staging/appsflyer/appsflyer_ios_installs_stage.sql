with

appsflyer as (

  select * from {{ ref('appsflyer_ios_installs_source') }}

)

select * from appsflyer