{{ config(
    materialized='tuple_incremental'
    , unique_key=['partition_date']
    , sort=['partition_date', 'hour']
    , dist='zip'
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , on_schema_change='append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with fct_vast_ads as (
  select * from {{ ref('fct_vast_ads_enriched') }}
)

, users as (
  select * from {{ ref('rails_prod_users_source') }}
)

select
  fct_vast_ads.partition_date
  , users.zip
  , users.dma_code
  , users.dma_region
  , users.dma_name
  , users.income
  , users.age_range
  , users.gender
  , fct_vast_ads.ad_reseller
  , extract(hour from fct_vast_ads.partition_date_hour)              as hour --noqa: L029
  , substring(
    substring(
      fct_vast_ads.sutured_pid
      , charindex('-', fct_vast_ads.sutured_pid) + 1
      , length(fct_vast_ads.sutured_pid)
    )
    , 0
    , charindex(
      '-', substring(fct_vast_ads.sutured_pid, charindex('-', fct_vast_ads.sutured_pid) + 1, length(fct_vast_ads.sutured_pid))
    )
  )                                                                  as device
  , case
    when fct_vast_ads.duration <= 0 then 'error - 0 or less'
    when fct_vast_ads.duration >= 1 and fct_vast_ads.duration <= 14 then 'less than 15'
    when fct_vast_ads.duration = 15 then '15'
    when fct_vast_ads.duration >= 16 and fct_vast_ads.duration <= 29 then 'between 15 and 30'
    when fct_vast_ads.duration = 30 then '30'
    when fct_vast_ads.duration >= 31 and fct_vast_ads.duration <= 59 then 'between 30 and 60'
    when fct_vast_ads.duration = 60 then '60'
    when fct_vast_ads.duration >= 61 then 'greater than 60'
  end                                                                as duration
  , sum(case when fct_vast_ads.is_empty = true then 1 else 0 end)    as empty_count
  , sum(case when fct_vast_ads.is_filled = true then 1 else 0 end)   as filled_count
  , sum(case when fct_vast_ads.is_inserted = true then 1 else 0 end) as inserted_count
  , sum(case when fct_vast_ads.is_ingested = true then 1 else 0 end) as ingested_count
  , sum(
    case
      when
        fct_vast_ads.is_fallback = false and fct_vast_ads.is_evergreen = false and fct_vast_ads.is_filled = true then 1
      else 0
    end
  )                                                                  as primary_count
  , sum(
    case
      when
        fct_vast_ads.is_fallback = true and fct_vast_ads.is_evergreen = false and fct_vast_ads.is_filled = true then 1
      else 0
    end
  )                                                                  as fallback_count
  , sum(
    case
      when
        fct_vast_ads.is_evergreen = true and fct_vast_ads.is_fallback = false and fct_vast_ads.is_filled = true then 1
      else 0
    end
  )                                                                  as evergreen_count
  , sum(
    case
      when
        fct_vast_ads.is_evergreen = true and fct_vast_ads.is_fallback = true and fct_vast_ads.is_filled = true then 1
      else 0
    end
  )                                                                  as evergreen_fallback_count
  , sum(
    case
      when
        fct_vast_ads.is_fallback = false and fct_vast_ads.is_evergreen = false and fct_vast_ads.is_inserted = true then 1 --noqa: L016
      else 0
    end
  )                                                                  as primary__inserted_count
  , sum(
    case
      when
        fct_vast_ads.is_fallback = true and fct_vast_ads.is_evergreen = false and fct_vast_ads.is_inserted = true then 1
      else 0
    end
  )                                                                  as fallback__inserted_count
  , sum(
    case
      when
        fct_vast_ads.is_evergreen = true and fct_vast_ads.is_fallback = false and fct_vast_ads.is_inserted = true then 1
      else 0
    end
  )                                                                  as evergreen__inserted_count
  , sum(
    case
      when
        fct_vast_ads.is_evergreen = true and fct_vast_ads.is_fallback = true and fct_vast_ads.is_inserted = true then 1
      else 0
    end
  )                                                                  as evergreen_fallback__inserted_count
from fct_vast_ads
join users
  on fct_vast_ads.user_id = users.user_id
where fct_vast_ads.partition_date between '{{ start_date }}' and '{{ end_date }}'
{{ dbt_utils.group_by(n=12) }}
order by 1, 2
