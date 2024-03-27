{{ config(materialized = 'table', sort = 'ad_date', tags=["daily", "exclude_hourly"]) }}
with

date_spine as ( --makes sure get all dates even if there is no spend

  select observation_date
  from {{ ref('dim_dates') }}
  where observation_date < getdate()

)

, first_pays as (

  select
    date_trunc('day', first_paid_at - interval '7 days') as attributed_conversion_day
    , count(distinct account_id)                         as conversions
  from {{ ref('fct_acquisition_funnel') }}
  {{ dbt_utils.group_by(n=1) }}

)

, revenue as (

  select
    date_trunc('day', first_payment_at - interval '7 days') as attributed_conversion_day
    , sum(ltv_revenue_one_month)                            as revenue_one_month
  from {{ ref('fct_lifetime_value') }}
  {{ dbt_utils.group_by(n=1) }}

)

, agg as (
  select
    ad_date
    , channel
    , sum(impressions) as impressions
    , sum(spend)       as spend
  from {{ ref('marketing_spend_consolidated_stage') }}
  {{ dbt_utils.group_by(n=2) }}

)

, impressions as (

  select *
  from (
    select
      channel
      , ad_date
      , impressions
    from agg
    ) pivot ( sum(impressions) for channel in (  --noqa
      'fb_prospecting_on_pre_idfa'
      , 'fb_prospecting_on_post_idfa'
      , 'fb_retargeting_pre_idfa'
      , 'fb_retargeting_post_idfa'
      , 'fb_prospecting_show_pre_idfa'
      , 'fb_prospecting_show_post_idfa'
      , 'google_brand'
      , 'google_other'
      , 'google_non_brand'
      , 'google_show'
      , 'amazon'
      , 'youtube'
      , 'cj'
      , 'tiktok'
      , 'roku_cpa'
      , 'roku_cpm'
      , 'apple'
      , 'acquisition_other'
      , 'brand'
      , 'direct_mail'
      , 'tv'
      , 'radio'
      , 'bing'
    )
  )

)

, spend as ( --noqa

  select *
  from (
    select
      channel
      , ad_date
      , spend
    from agg
    ) pivot ( sum(coalesce(spend, 0)) for channel in (  --noqa
      'fb_prospecting_on_pre_idfa'
      , 'fb_prospecting_on_post_idfa'
      , 'fb_retargeting_pre_idfa'
      , 'fb_retargeting_post_idfa'
      , 'fb_prospecting_show_pre_idfa'
      , 'fb_prospecting_show_post_idfa'
      , 'google_brand'
      , 'google_other'
      , 'google_non_brand'
      , 'google_show'
      , 'amazon'
      , 'youtube'
      , 'cj'
      , 'tiktok'
      , 'roku_cpa'
      , 'roku_cpm'
      , 'apple'
      , 'acquisition_other'
      , 'brand'
      , 'direct_mail'
      , 'tv'
      , 'radio'
      , 'bing'
    )
  )

)

select
  date_spine.observation_date                 as ad_date
  , spend.fb_prospecting_on_pre_idfa          as fb_prospecting_on_pre_idfa_s
  , spend.fb_prospecting_on_post_idfa         as fb_prospecting_on_post_idfa_s
  , impressions.fb_prospecting_on_pre_idfa    as fb_prospecting_on_pre_idfa_i
  , impressions.fb_prospecting_on_post_idfa   as fb_prospecting_on_post_idfa_i
  , spend.fb_retargeting_pre_idfa             as fb_retargeting_pre_idfa_s
  , spend.fb_retargeting_post_idfa            as fb_retargeting_post_idfa_s
  , impressions.fb_retargeting_pre_idfa       as fb_retargeting_pre_idfa_i
  , impressions.fb_retargeting_post_idfa      as fb_retargeting_post_idfa_i
  , spend.fb_prospecting_show_pre_idfa        as fb_prospecting_show_pre_pre_s
  , spend.fb_prospecting_show_post_idfa       as fb_prospecting_show_post_idfa_s
  , impressions.fb_prospecting_show_pre_idfa  as fb_prospecting_show_pre_idfa_i
  , impressions.fb_prospecting_show_post_idfa as fb_prospecting_show_post_idfa_i
  , spend.google_brand                        as google_brand_s
  , impressions.google_brand                  as google_brand_i
  , spend.amazon                              as amazon_s
  , impressions.amazon                        as amazon_i
  , spend.cj                                  as cj_s
  , impressions.cj                            as cj_i
  , spend.tiktok                              as tiktok_s
  , impressions.tiktok                        as tiktok_i
  , spend.apple                               as apple_s
  , impressions.apple                         as apple_i
  , spend.youtube                             as youtube_s
  , impressions.youtube                       as youtube_i
  , spend.brand                               as brand_s
  , impressions.brand                         as brand_i
  , spend.tv                                  as tv_s
  , impressions.tv                            as tv_i
  , spend.roku_cpa                            as roku_cpa_s
  , impressions.roku_cpa                      as roku_cpa_i
  , spend.roku_cpm                            as roku_s
  , impressions.roku_cpm                      as roku_i
  , spend.google_non_brand                    as google_non_brand_s
  , impressions.google_non_brand              as google_non_brand_i
  , spend.google_other                        as google_other_brand_s
  , impressions.google_other                  as google_other_brand_i
  , spend.google_show                         as google_show_s
  , impressions.google_show                   as google_show_i
  , spend.acquisition_other                   as acquisition_other_s
  , impressions.acquisition_other             as acquisition_other_i
  , spend.radio                               as radio_s
  , impressions.radio                         as radio_i
  , spend.bing                                as bing_s
  , impressions.bing                          as bing_i
  , spend.direct_mail                         as direct_mail_s
  , impressions.direct_mail                   as direct_mail_i
  , first_pays.conversions
  , revenue.revenue_one_month
from date_spine
left join spend on (date_spine.observation_date = spend.ad_date)
left join first_pays on (date_spine.observation_date = first_pays.attributed_conversion_day)
left join impressions on (date_spine.observation_date = impressions.ad_date)
left join revenue on (date_spine.observation_date = revenue.attributed_conversion_day)
order by 1 asc