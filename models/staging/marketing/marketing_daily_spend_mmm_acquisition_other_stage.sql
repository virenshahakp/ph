with

marketing_daily_spend as (

  select * 
  from {{ ref('fct_marketing_daily_spend') }}
  where spend is not null
    and spend > 0
    and partner not ilike 'adwords' -- using API
    and partner not ilike 'adwords_uac' -- uploaded the data
    and partner not ilike 'adwords_discovery' -- uploaded the data
    and partner not ilike 'facebook' -- using API
    and partner not ilike 'facebook_co_spend' --using API
    and partner not ilike 'roku_display'
    and partner not ilike 'roku_other'
    and partner not ilike 'amazon_6_month_offer'
    and partner not ilike 'apple_search'
    and partner not ilike 'bing'
    and partner not ilike 'pinterest'
    and partner not ilike 'liftoff_%' -- right now it is mobile but this is future proofing
    and partner not ilike 'snapchat' 
    and partner not ilike 'tiktok'
    and partner not ilike 'youtube'
    and partner not ilike 'youtube_co_spend'
    and partner not ilike 'cj'
    and partner not ilike 'appsflyer' 
    and partner not ilike 'fee_2021' --vendor fees that we are removing
    and partner not ilike 'billo'
    and partner not ilike 'wideout'
    and partner not ilike 'liveramp'
    and partner not ilike 'simulmedia' --uploaded the data
    and partner not ilike 'tatari'
    and partner not ilike 'firetv_display'
    
)

select 
  ad_date
  , spend as spend
  , case 
    when partner ilike 'speedeon' or partner ilike 'share_local_mailer'
      then 'direct_mail'
    when  partner  ilike 'eichoff_tv'
      then 'tv'
    when partner ilike 'local_radio'
      or partner ilike 'xm' 
      or partner ilike 'pandora'
      then 'radio'
    when
      partner  ilike 'partner'
      or partner  ilike 'partner_amc'
      or partner  ilike 'partner_viacom'
      or partner  ilike 'audioboom' 
      or partner  ilike 'tvone'
      or partner  ilike 'influencers' 
      or partner  ilike 'spotify'
      or partner  ilike 'branded'
      or partner  ilike 'geistm'
      or partner  ilike 'mymove' 
      or partner  ilike 'groupon_b_c_d'
      or partner  ilike 'barnes_and_noble' 
      or partner  ilike 'posterscope'
      or partner  ilike 'vengo'
      or partner  ilike 'ink'
      or partner  ilike 'military_makeover'
      or partner  ilike 'bouncex' 
      or partner  ilike 'undertone'
      or partner  ilike 'mobile' 
      or partner  ilike 'adquick_outdoor'
      or partner  ilike 'pch'
      or partner  ilike 'mightyhive'
      or partner  ilike 'quora'
      or partner  ilike 'octopus'
      or partner  ilike 'wetv' 
      or partner  ilike 'viacom'
      or partner  ilike 'hallmark' 
      or partner  ilike 'vice'
      or partner  ilike 'bet' 
      or partner  ilike 'frankly_media'
      or partner  ilike 'nytimes'
      or partner  ilike 'kin_community'
      or partner  ilike 'headgum'
      or partner  ilike 'ne_milli' 
      or partner  ilike 'adcology'
      or partner  ilike 'insp'
      or partner  ilike 'encompass'
      or partner  ilike 'fye' 
      or partner  ilike 'co_spend_credit'
      or partner  ilike 'landau'
      or partner  ilike 'genius' 
      then 'brand'
    else 'acquisition_other'
  end as channel
from marketing_daily_spend

