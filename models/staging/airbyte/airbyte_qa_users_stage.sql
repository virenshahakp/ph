with

qa_users as (

  select * from {{ ref('airbyte_qa_users_source') }}

)

select * from qa_users
