{{ config(
    materialized='tuple_incremental'
    , unique_key=['report_timezone'
      ,'report_date'
      ]
    , sort=['report_timezone'
        , 'report_date'
    ]
    , dist='even'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_schema_change = 'append_new_columns'
) }}

{% set end_date = var("end_date") %}
{% set start_date = var("start_date") %}
{% set default_lookback_days = 10 %}

{% if start_date == "" and end_date == "" %}

{% set end_date = run_started_at.astimezone(modules.pytz.timezone("UTC")) %}
{% set start_date = (end_date - modules.datetime.timedelta(default_lookback_days)) %}
{% set end_date = end_date.date() %}
{% set start_date = start_date.date() %}


{% endif %}


with ad_data as (
  select * 
  from {{ ref('fct_impressions_agg') }}
)

, minutes_watched as (
  select * 
  from {{ ref('fct_watched_minutes_agg') }}
)

, totals as (
  select
    ad_data.report_timezone
    , ad_data.report_date
    , ad_data.impressions as impressions
    , ad_data.ad_revenue as ad_revenue
    , ad_data.ad_unit_length as ad_unit_length
    , minutes_watched.watched_minutes::float / 60::float as watched_hours
  from ad_data
  inner join minutes_watched
    on minutes_watched.report_date = ad_data.report_date
      and ad_data.report_timezone = minutes_watched.report_timezone
  where minutes_watched.report_date::date between date_add('days', -61, '{{ start_date }}') and '{{ end_date }}'
)

, agg as (
  select
    report_timezone
    , report_date::date
    , impressions
    , ad_revenue
    , ad_unit_length
    , watched_hours
    
    , sum(
      impressions
    ) over (
      partition by report_timezone order by report_date rows between 6 preceding and current row
    ) as impressions_7_day
    , sum(
      impressions
    ) over (
      partition by report_timezone order by report_date rows between 29 preceding and current row
    ) as impressions_30_day
    
    , sum(
      ad_revenue
    ) over (
      partition by report_timezone order by report_date rows between 6 preceding and current row
    ) as ad_revenue_7_day
    , sum(
      ad_revenue
    ) over (
      partition by report_timezone order by report_date rows between 29 preceding and current row
    ) as ad_revenue_30_day

    , sum(
      watched_hours
    ) over (
      partition by report_timezone order by report_date rows between 6 preceding and current row
    ) as watched_hours_7_day
    , sum(
      watched_hours
    ) over (
      partition by report_timezone order by report_date rows between 29 preceding and current row
    ) as watched_hours_30_day
  from totals
)

, lag_frames as (
  select 
    *
    , lag(impressions, 1) over (partition by report_timezone order by report_date) as impressions__yesterday
    , lag(
      impressions_7_day, 7
    ) over (partition by report_timezone order by report_date) as impressions_7_day__previous_7_day
    , lag(
      impressions_30_day, 30
    ) over (partition by report_timezone order by report_date) as impressions_30_day__previous_30_day
    
    , lag(ad_revenue, 1) over (partition by report_timezone order by report_date) as ad_revenue__yesterday
    , lag(
      ad_revenue_7_day, 7
    ) over (partition by report_timezone order by report_date) as ad_revenue_7_day__previous_7_day
    , lag(
      ad_revenue_30_day, 30
    ) over (partition by report_timezone order by report_date) as ad_revenue_30_day__previous_30_day

    , lag(watched_hours, 1) over (partition by report_timezone order by report_date) as watched_hours__yesterday
    , lag(
      watched_hours_7_day, 7
    ) over (partition by report_timezone order by report_date) as watched_hours_7_day__previous_7_day
    , lag(
      watched_hours_30_day, 30
    ) over (partition by report_timezone order by report_date) as watched_hours_30_day__previous_30_day
  from agg
)

select
  report_timezone
  , report_date
  , impressions
  , impressions__yesterday
  , impressions_7_day
  , impressions_7_day__previous_7_day
  , impressions_30_day
  , impressions_30_day__previous_30_day
  , ad_revenue
  , ad_revenue__yesterday
  , ad_revenue_7_day
  , ad_revenue_7_day__previous_7_day
  , ad_revenue_30_day
  , ad_revenue_30_day__previous_30_day

  , watched_hours
  , watched_hours__yesterday
  , watched_hours_7_day
  , watched_hours_7_day__previous_7_day
  , watched_hours_30_day
  , watched_hours_30_day__previous_30_day
  , case
    when
      sum(
        impressions::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(impressions::float) over (partition by report_timezone, report_date) is null then null
    else impressions::float / sum(impressions::float) over (partition by report_timezone, report_date)
  end as impressions__1_day_percent_of_total
  , case
    when
      impressions__yesterday = 0 then null
    else (impressions::float - impressions__yesterday::float) / impressions__yesterday::float
  end as impressions__percent_difference_from__yesterday
  , case
    when
      sum(
        impressions_7_day::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(impressions_7_day::float) over (partition by report_timezone, report_date) is null then null
    else impressions_7_day::float / sum(impressions_7_day::float) over (partition by report_timezone, report_date)
  end as impressions__7_day_percent_of_total
  , case
    when
      impressions_7_day__previous_7_day = 0 then null
    else
      (impressions_7_day::float - impressions_7_day__previous_7_day::float) / impressions_7_day__previous_7_day::float
  end as impressions_7_day__percent_difference_from__previous_7_day
  , case
    when
      sum(
        impressions_30_day::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(impressions_30_day::float) over (partition by report_timezone, report_date) is null then null
    else impressions_30_day::float / sum(impressions_30_day::float) over (partition by report_timezone, report_date)
  end as impressions__30_day_percent_of_total
  , case
    when
      impressions_30_day__previous_30_day = 0 then null
    else
      (
        impressions_30_day::float - impressions_30_day__previous_30_day::float
      ) / impressions_30_day__previous_30_day::float
  end as impressions_30_day__percent_difference_from__previous_30_day

  , case
    when
      sum(
        ad_revenue::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(ad_revenue::float) over (partition by report_timezone, report_date) is null then null
    else ad_revenue::float / sum(ad_revenue::float) over (partition by report_timezone, report_date)
  end as ad_revenue__1_day_percent_of_total
  , case
    when
      ad_revenue__yesterday = 0 then null
    else (ad_revenue::float - ad_revenue__yesterday::float) / ad_revenue__yesterday::float
  end as ad_revenue__percent_difference_from__yesterday
  , case
    when
      sum(
        ad_revenue_7_day::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(ad_revenue_7_day::float) over (partition by report_timezone, report_date) is null then null
    else ad_revenue_7_day::float / sum(ad_revenue_7_day::float) over (partition by report_timezone, report_date)
  end as ad_revenue__7_day_percent_of_total
  , case
    when
      ad_revenue_7_day__previous_7_day = 0 then null
    else (ad_revenue_7_day::float - ad_revenue_7_day__previous_7_day::float) / ad_revenue_7_day__previous_7_day::float
  end as ad_revenue_7_day__percent_difference_from__previous_7_day
  , case
    when
      sum(
        ad_revenue_30_day::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(ad_revenue_30_day::float) over (partition by report_timezone, report_date) is null then null
    else ad_revenue_30_day::float / sum(ad_revenue_30_day::float) over (partition by report_timezone, report_date)
  end as ad_revenue__30_day_percent_of_total
  , case
    when
      ad_revenue_30_day__previous_30_day = 0 then null
    else
      (ad_revenue_30_day::float - ad_revenue_30_day__previous_30_day::float) / ad_revenue_30_day__previous_30_day::float
  end as ad_revenue_30_day__percent_difference_from__previous_30_day
  , case
    when
      sum(
        watched_hours::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(watched_hours::float) over (partition by report_timezone, report_date) is null then null
    else watched_hours::float / sum(watched_hours::float) over (partition by report_timezone, report_date)
  end as watched_hours__1_day_percent_of_total
  , case
    when
      watched_hours__yesterday = 0 then null
    else (watched_hours::float - watched_hours__yesterday::float) / watched_hours__yesterday::float
  end as watched_hours__percent_difference_from__yesterday
  , case
    when
      sum(
        watched_hours_7_day::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(watched_hours_7_day::float) over (partition by report_timezone, report_date) is null then null
    else watched_hours_7_day::float / sum(watched_hours_7_day::float) over (partition by report_timezone, report_date)
  end as watched_hours__7_day_percent_of_total
  , case
    when
      watched_hours_7_day__previous_7_day = 0 then null
    else
      (
        watched_hours_7_day::float - watched_hours_7_day__previous_7_day::float
      ) / watched_hours_7_day__previous_7_day::float
  end as watched_hours_7_day__percent_difference_from__previous_7_day
  , case
    when
      sum(
        watched_hours_30_day::float
      ) over (
        partition by report_timezone, report_date
      ) = 0 or sum(watched_hours_30_day::float) over (partition by report_timezone, report_date) is null then null
    else watched_hours_30_day::float / sum(watched_hours_30_day::float) over (partition by report_timezone, report_date)
  end as watched_hours__30_day_percent_of_total
  , case
    when
      watched_hours_30_day__previous_30_day = 0 then null
    else
      (
        watched_hours_30_day::float - watched_hours_30_day__previous_30_day::float
      ) / watched_hours_30_day__previous_30_day::float
  end as watched_hours_30_day__percent_difference_from__previous_30_day

  , impressions::float / nullif(watched_hours, 0)::float as impressions_per_watched_hour
  , ad_revenue::float / nullif(watched_hours, 0)::float as revenue_per_watched_hour
from lag_frames
where report_date between '{{ start_date }}' and '{{ end_date }}'

