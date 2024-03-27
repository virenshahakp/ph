{{ config(
    materialized='tuple_incremental'
    , unique_key=['report_timezone'
      ,'report_date']
    , sort=[ 'report_timezone'
        , 'report_date'
        , 'is_paid'
        , 'is_count'
        , 'sov'
        , 'bidder_name'
    ]
    , dist='even'
    , full_refresh = false
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_schema_change = 'append_new_columns'
) }}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with report_data as (
  select
    report_timezone
    , report_date
    , bidder_name
    , is_paid
    , is_count
    , sov
    , impressions
    , ad_revenue
    , ad_seconds
    , (ad_revenue * 1000) / impressions as ecpm
    , case 
      when 
        ad_seconds::float = 0 
        then 0 
      when 
        ad_seconds::float is null 
        then null 
      else ecpm::float / ad_seconds::float 
    end as ecpm_per_impression_second

    , case 
      when 
        impressions::float = 0 
        then 0 
      when 
        impressions::float is null 
        then null 
      else ad_revenue::float / impressions::float 
    end as ave_revenue_per_impression
    
    , case 
      when 
        ad_seconds::float = 0 
        then 0 
      when 
        ad_seconds::float is null 
        then null 
      else ave_revenue_per_impression::float / ad_seconds::float 
    end as ave_revenue_per_impression_second

    , case 
      when 
        ad_seconds::float = 0 
        then 0 
      when 
        ad_seconds::float is null 
        then null 
      else ecpm::float / ad_seconds::float 
    end as ecpm_per_impression_second

    --lookback windows
    , sum(impressions) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
      rows between 6 preceding and current row
    ) 
    as impressions_7_day

    , sum(ad_revenue) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date 
      rows between 6 preceding and current row
    ) 
    as ad_revenue_7_day

    , sum(impressions) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date 
      rows between 29 preceding and current row
    ) 
    as impressions_30_day


    , sum(ad_revenue) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
      rows between 29 preceding and current row
    ) 
    as ad_revenue_30_day
  from {{ ref('fct_ad_pulse_bidders_pre') }}
  where report_date between date_add('days', -61, '{{ start_date }}') and '{{ end_date }}'
)

, lag_frames as (
  select 
    *
    , lag(impressions, 1) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
    ) 
    as impressions__previous_1_day

    , lag(impressions_7_day, 7) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
    ) 
    as impressions_7_day__previous_7_day
        
    , lag(impressions_30_day, 30) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
    ) 
    as impressions_30_day__previous_30_day

    , lag(ad_revenue, 1) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
    ) 
    as ad_revenue__previous_1_day

    , lag(ad_revenue_7_day, 7) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
    ) 
    as ad_revenue_7_day__previous_7_day

    , lag(ad_revenue_30_day, 30) 
    over (
      partition by report_timezone
        , bidder_name
        , is_paid 
        , is_count
        , sov
      order by report_date
    ) 
    as ad_revenue_30_day__previous_30_day
          
  from report_data
)

, final as (
  select
    report_timezone
    , report_date
    , bidder_name
    , is_paid
    , is_count
    , sov
    , ad_seconds
    , impressions
    , impressions__previous_1_day
    , impressions_7_day

    , impressions_7_day__previous_7_day

    , impressions_30_day
    , impressions_30_day__previous_30_day
    , ad_revenue
    , ad_revenue__previous_1_day
  
    , ad_revenue_7_day
    , ad_revenue_7_day__previous_7_day
    , ad_revenue_30_day
    , ad_revenue_30_day__previous_30_day

    , case 
      when 
        sum(impressions::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) = 0 
        or sum(impressions::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) is null 
        then null 
      else 
        impressions::float / sum(impressions::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) 
    end as impressions__1_day_percent_of_total
    , case 
      when 
        impressions__previous_1_day = 0 
        then null 
      else 
        (impressions::float - impressions__previous_1_day::float) / impressions__previous_1_day::float 
    end as impressions__percent_difference_from__previous_1_day
    , case 
      when 
        sum(impressions_7_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) = 0 
        or sum(impressions_7_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) is null 
        then null 
      else 
        impressions_7_day::float / sum(impressions_7_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) 
    end as impressions__7_day_percent_of_total

    , case 
      when 
        impressions_7_day__previous_7_day = 0 
        then null 
      else 
        (
          impressions_7_day::float - impressions_7_day__previous_7_day::float
        ) / impressions_7_day__previous_7_day::float 
    end as impressions_7_day__percent_difference_from__previous_7_day
  
    , case 
      when 
        sum(impressions_30_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) = 0 
        or sum(impressions_30_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) is null 
        then null 
      else 
        impressions_30_day::float / sum(impressions_30_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) 
    end as impressions__30_day_percent_of_total
    , case 
      when 
        impressions_30_day__previous_30_day = 0 
        then null 
      else 
        (
          impressions_30_day::float - impressions_30_day__previous_30_day::float
        ) / impressions_30_day__previous_30_day::float 
    end as impressions_30_day__percent_difference_from__previous_30_day
    , case 
      when 
        sum(ad_revenue::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) = 0 
        or sum(ad_revenue::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) is null 
        then null 
      else 
        ad_revenue::float / sum(ad_revenue::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) 
    end as ad_revenue__1_day_percent_of_total

    , case 
      when 
        ad_revenue__previous_1_day = 0 
        then null 
      else 
        (ad_revenue::float - ad_revenue__previous_1_day::float) / ad_revenue__previous_1_day::float 
    end as ad_revenue__percent_difference_from__previous_1_day
  
    , case 
      when 
        sum(ad_revenue_7_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) = 0 
        or sum(ad_revenue_7_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) is null 
        then null 
      else 
        ad_revenue_7_day::float / sum(ad_revenue_7_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) 
    end as ad_revenue__7_day_percent_of_total
    , case 
      when 
        ad_revenue_7_day__previous_7_day = 0 
        then null 
      else 
        (ad_revenue_7_day::float - ad_revenue_7_day__previous_7_day::float) / ad_revenue_7_day__previous_7_day::float 
    end as ad_revenue_7_day__percent_difference_from__previous_7_day
    , case 
      when 
        sum(ad_revenue_30_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) = 0 
        or sum(ad_revenue_30_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) is null 
        then null 
      else 
        ad_revenue_30_day::float / sum(ad_revenue_30_day::float) 
        over (
          partition by report_timezone
            , report_date
            , is_paid
            , is_count
            , sov
        ) 
    end as ad_revenue__30_day_percent_of_total
    , case 
      when 
        ad_revenue_30_day__previous_30_day = 0 
        then null 
      else 
        (
          ad_revenue_30_day::float - ad_revenue_30_day__previous_30_day::float
        ) / ad_revenue_30_day__previous_30_day::float 
    end as ad_revenue_30_day__percent_difference_from__previous_30_day
  from lag_frames
)

select *
from final
where report_date between '{{ start_date }}' and '{{ end_date }}'
