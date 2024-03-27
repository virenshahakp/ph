with

agents as (

  select * from {{ ref('zendesk_zd_agents_source') }}

)

select * from agents