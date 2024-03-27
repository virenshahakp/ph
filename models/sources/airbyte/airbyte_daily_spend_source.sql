with

spend as (

  select * from {{ source('airbyte', 'daily_spend') }}

)

, renamed as (

  -- we need quotes around many columns, so disabling the special characters rule to keep things consistent
  -- noqa: disable=RF05,RF06
  select
    date::date                                                                          as ad_date
    -- transformation
    -- 1: remove white space
    -- 2: if empty string, cast as null
    -- 3: convert to number
    , to_number(nullif(replace("adwords", ' ', ''), ''), 'FM9G999G999D99')              as adwords
    , to_number(nullif(replace("youtube", ' ', ''), ''), 'FM9G999G999D99')              as youtube
    , to_number(nullif(replace("adwords uac", ' ', ''), ''), 'FM9G999G999D99')          as adwords_uac
    , to_number(nullif(replace("adwords discovery", ' ', ''), ''), 'FM9G999G999D99')    as adwords_discovery
    , to_number(nullif(replace("facebook", ' ', ''), ''), 'FM9G999G999D99')             as facebook
    , to_number(nullif(replace("programmatic", ' ', ''), ''), 'FM9G999G999D99')         as programmatic
    , to_number(nullif(replace("bing", ' ', ''), ''), 'FM9G999G999D99')                 as bing
    , to_number(nullif(replace("yahoo", ' ', ''), ''), 'FM9G999G999D99')                as yahoo
    , to_number(nullif(replace("reddit", ' ', ''), ''), 'FM9G999G999D99')               as reddit
    , to_number(nullif(replace("roku display", ' ', ''), ''), 'FM9G999G999D99')         as roku_display
    , to_number(nullif(replace("roku other", ' ', ''), ''), 'FM9G999G999D99')           as roku_other
    , to_number(nullif(replace("firetv display", ' ', ''), ''), 'FM9G999G999D99')       as firetv_display
    , to_number(nullif(replace("firetv other (oobe)", ' ', ''), ''), 'FM9G999G999D99')  as firetv_other_oobe
    , to_number(nullif(replace("amazon 6 month offer", ' ', ''), ''), 'FM9G999G999D99') as amazon_6_month_offer
    , to_number(nullif(replace("cj", ' ', ''), ''), 'FM9G999G999D99')                   as cj
    , to_number(nullif(replace("partner", ' ', ''), ''), 'FM9G999G999D99')              as partner
    , to_number(nullif(replace("partner (amc)", ' ', ''), ''), 'FM9G999G999D99')        as partner_amc
    , to_number(nullif(replace("partner (viacom)", ' ', ''), ''), 'FM9G999G999D99')     as partner_viacom
    , to_number(nullif(replace("audioboom", ' ', ''), ''), 'FM9G999G999D99')            as audioboom
    , to_number(nullif(replace("tvone", ' ', ''), ''), 'FM9G999G999D99')                as tvone
    , to_number(nullif(replace("influencers", ' ', ''), ''), 'FM9G999G999D99')          as influencers
    , to_number(nullif(replace("spotify", ' ', ''), ''), 'FM9G999G999D99')              as spotify
    , to_number(nullif(replace("podcast", ' ', ''), ''), 'FM9G999G999D99')              as podcast
    , to_number(nullif(replace("branded", ' ', ''), ''), 'FM9G999G999D99')              as branded
    , to_number(nullif(replace("radio", ' ', ''), ''), 'FM9G999G999D99')                as radio
    , to_number(nullif(replace("geistm", ' ', ''), ''), 'FM9G999G999D99')               as geistm
    , to_number(nullif(replace("eichoff tv", ' ', ''), ''), 'FM9G999G999D99')           as eichoff_tv
    , to_number(nullif(replace("mymove", ' ', ''), ''), 'FM9G999G999D99')               as mymove
    , to_number(nullif(replace("speedeon", ' ', ''), ''), 'FM9G999G999D99')             as speedeon
    , to_number(nullif(replace("groupon b/c/d", ' ', ''), ''), 'FM9G999G999D99')        as groupon_b_c_d
    , to_number(nullif(replace("share local mailer", ' ', ''), ''), 'FM9G999G999D99')   as share_local_mailer
    , to_number(nullif(replace("pinterest", ' ', ''), ''), 'FM9G999G999D99')            as pinterest
    , to_number(nullif(replace("barnes & noble", ' ', ''), ''), 'FM9G999G999D99')       as barnes_and_noble
    , to_number(nullif(replace("posterscope", ' ', ''), ''), 'FM9G999G999D99')          as posterscope
    , to_number(nullif(replace("upfront influencers", ' ', ''), ''), 'FM9G999G999D99')  as upfront_influencers
    , to_number(nullif(replace("vengo", ' ', ''), ''), 'FM9G999G999D99')                as vengo
    , to_number(nullif(replace("cnet newsletter", ' ', ''), ''), 'FM9G999G999D99')      as cnet_newsletter
    , to_number(nullif(replace("cardlytics", ' ', ''), ''), 'FM9G999G999D99')           as cardlytics
    , to_number(nullif(replace("ink", ' ', ''), ''), 'FM9G999G999D99')                  as ink
    , to_number(nullif(replace("military makeover", ' ', ''), ''), 'FM9G999G999D99')    as military_makeover
    , to_number(nullif(replace("bouncex", ' ', ''), ''), 'FM9G999G999D99')              as bouncex
    , to_number(nullif(replace("undertone", ' ', ''), ''), 'FM9G999G999D99')            as undertone
    , to_number(nullif(replace("mobile", ' ', ''), ''), 'FM9G999G999D99')               as mobile
    , to_number(nullif(replace("adquick (outdoor)", ' ', ''), ''), 'FM9G999G999D99')    as adquick_outdoor
    , to_number(nullif(replace("apple search", ' ', ''), ''), 'FM9G999G999D99')         as apple_search
    , to_number(nullif(replace("pch", ' ', ''), ''), 'FM9G999G999D99')                  as pch
    , to_number(nullif(replace("liveintent", ' ', ''), ''), 'FM9G999G999D99')           as liveintent
    , to_number(nullif(replace("mightyhive", ' ', ''), ''), 'FM9G999G999D99')           as mightyhive
    , to_number(nullif(replace("snapchat", ' ', ''), ''), 'FM9G999G999D99')             as snapchat
    , to_number(nullif(replace("tiktok", ' ', ''), ''), 'FM9G999G999D99')               as tiktok
    , to_number(nullif(replace("liftoff mobile", ' ', ''), ''), 'FM9G999G999D99')       as liftoff_mobile
    , to_number(nullif(replace("outbrain", ' ', ''), ''), 'FM9G999G999D99')             as outbrain
    , to_number(nullif(replace("best buy", ' ', ''), ''), 'FM9G999G999D99')             as best_buy
    , to_number(nullif(replace("twitter", ' ', ''), ''), 'FM9G999G999D99')              as twitter
    , to_number(nullif(replace("pandora", ' ', ''), ''), 'FM9G999G999D99')              as pandora
    , to_number(nullif(replace("local radio", ' ', ''), ''), 'FM9G999G999D99')          as local_radio
    , to_number(nullif(replace("other", ' ', ''), ''), 'FM9G999G999D99')                as other
    , to_number(nullif(replace("quora", ' ', ''), ''), 'FM9G999G999D99')                as quora
    , to_number(nullif(replace("simulmedia", ' ', ''), ''), 'FM9G999G999D99')           as simulmedia
    , to_number(nullif(replace("sirius xm", ' ', ''), ''), 'FM9G999G999D99')            as xm
    , to_number(nullif(replace("octopus", ' ', ''), ''), 'FM9G999G999D99')              as octopus
    , to_number(nullif(replace("youtube (co-spend", ' ', ''), ''), 'FM9G999G999D99')    as youtube_co_spend
    , to_number(nullif(replace("facebook (co-spend)", ' ', ''), ''), 'FM9G999G999D99')  as facebook_co_spend
    , to_number(nullif(replace("wetv", ' ', ''), ''), 'FM9G999G999D99')                 as wetv
    , to_number(nullif(replace("viacom", ' ', ''), ''), 'FM9G999G999D99')               as viacom
    , to_number(nullif(replace("hallmark", ' ', ''), ''), 'FM9G999G999D99')             as hallmark
    , to_number(nullif(replace("vice", ' ', ''), ''), 'FM9G999G999D99')                 as vice
    , to_number(nullif(replace("bet", ' ', ''), ''), 'FM9G999G999D99')                  as bet
    , to_number(nullif(replace("frankly media", ' ', ''), ''), 'FM9G999G999D99')        as frankly_media
    , to_number(nullif(replace("nytimes", ' ', ''), ''), 'FM9G999G999D99')              as nytimes
    , to_number(nullif(replace("kin community", ' ', ''), ''), 'FM9G999G999D99')        as kin_community
    , to_number(nullif(replace("headgum", ' ', ''), ''), 'FM9G999G999D99')              as headgum
    , to_number(nullif(replace("1milli", ' ', ''), ''), 'FM9G999G999D99')               as one_milli
    , to_number(nullif(replace("hello fresh", ' ', ''), ''), 'FM9G999G999D99')          as hello_fresh
    , to_number(nullif(replace("adcology", ' ', ''), ''), 'FM9G999G999D99')             as adcology
    , to_number(nullif(replace("tatari", ' ', ''), ''), 'FM9G999G999D99')               as tatari
    , to_number(nullif(replace("insp", ' ', ''), ''), 'FM9G999G999D99')                 as insp
    , to_number(nullif(replace("encompass", ' ', ''), ''), 'FM9G999G999D99')            as encompass
    , to_number(nullif(replace("fye", ' ', ''), ''), 'FM9G999G999D99')                  as fye
    , to_number(nullif(replace("appsflyer", ' ', ''), ''), 'FM9G999G999D99')            as appsflyer
    , to_number(nullif(replace("fee 2021", ' ', ''), ''), 'FM9G999G999D99')             as fee_2021
    , to_number(nullif(replace("co-spend credit", ' ', ''), ''), 'FM9G999G999D99')      as co_spend_credit
    , to_number(nullif(replace("nextdoor", ' ', ''), ''), 'FM9G999G999D99')             as nextdoor
    , to_number(nullif(replace("taboola", ' ', ''), ''), 'FM9G999G999D99')              as taboola
    , to_number(nullif(replace("landau", ' ', ''), ''), 'FM9G999G999D99')               as landau
    , to_number(nullif(replace("billo", ' ', ''), ''), 'FM9G999G999D99')                as billo
    , to_number(nullif(replace("wideout", ' ', ''), ''), 'FM9G999G999D99')              as wideout
    , to_number(nullif(replace("liveramp", ' ', ''), ''), 'FM9G999G999D99')             as liveramp
    , to_number(nullif(replace("genius", ' ', ''), ''), 'FM9G999G999D99')               as genius
    , to_number(nullif(replace("rokt", ' ', ''), ''), 'FM9G999G999D99')                 as rokt
    , to_number(nullif(replace("simplifi", ' ', ''), ''), 'FM9G999G999D99')             as simplifi
    , to_number(nullif(replace("creatorly", ' ', ''), ''), 'FM9G999G999D99')            as creatorly
    , to_number(nullif(replace("channel ad credits", ' ', ''), ''), 'FM9G999G999D99')   as channel_ad_credits
    , to_number(nullif(replace("usa today", ' ', ''), ''), 'FM9G999G999D99')            as usa_today
    , to_number(nullif(replace("apple news", ' ', ''), ''), 'FM9G999G999D99')           as apple_news
    , to_number(nullif(replace("samsung ads", ' ', ''), ''), 'FM9G999G999D99')          as samsung_ads
    , to_number(nullif(replace("podcorn (audacy)", ' ', ''), ''), 'FM9G999G999D99')     as podcorn_audacy
    , to_number(nullif(replace("xm podcast", ' ', ''), ''), 'FM9G999G999D99')           as xm_podcast
  from spend
  where date::date <= current_date

)

select * from renamed
