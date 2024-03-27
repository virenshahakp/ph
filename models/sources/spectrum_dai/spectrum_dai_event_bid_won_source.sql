with

bid_requested as (

  select * from {{ source('spectrum_dai', 'event_bid_won') }}
    
  {%- if target.name != 'prod' %}
    where 1 = 1
      and partition_date >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
  {%- endif %}


)

, renamed as (

  select *
  from bid_requested

)

select * from renamed
