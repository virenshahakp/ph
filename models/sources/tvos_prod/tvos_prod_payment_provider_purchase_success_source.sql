with

payment_provider_purchase_success as (

  select * from {{ source('tvos_prod', 'payment_provider_purchase_success') }}

)

, renamed as (

  select
    {{ apple_common_columns() }}
    , sku
    , provider_name
  from payment_provider_purchase_success

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
