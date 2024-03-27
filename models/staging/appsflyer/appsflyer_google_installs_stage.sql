with

appsflyer as (

  select * from {{ ref('appsflyer_google_installs_source') }}

)

select * from appsflyer