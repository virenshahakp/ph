with

ad_network as (

  select * from {{ source('airbyte', 'ad_revenue_by_network') }}
)

, remove_non_numeric as (

  select
    month                                     as ad_month
    -- transformation
    -- 1: remove dollar sign
    , replace("a&e", '$', '')                 as aestr
    , replace(amc, '$', '')                   as amcstr
    , replace(axs, '$', '')                   as axs
    , replace(bet, '$', '')                   as bethd
    , replace(diy, '$', '')                   as diyhd
    , replace(fyi, '$', '')                   as fyistr
    , replace(gac, '$', '')                   as gachd
    , replace(gsn, '$', '')                   as gsnhd
    , replace(ifc, '$', '')                   as ifcstr
    , replace(mtv, '$', '')                   as mtvhd
    , replace(own, '$', '')                   as ownhd
    , replace(tlc, '$', '')                   as tlchd
    , replace(vh1, '$', '')                   as vh1hd
    , replace(cmtv, '$', '')                  as cmtvhd
    , replace(cleo, '$', '')                  as cleo
    , replace(food, '$', '')                  as foodhd
    , replace(hgtv, '$', '')                  as hgtvd
    , replace(insp, '$', '')                  as insphd
    , replace(logo, '$', '')                  as logohd
    , replace(mtv2, '$', '')                  as mtv2hd
    , replace(uptv, '$', '')                  as uphd
    , replace(vice, '$', '')                  as vicestr
    , replace(nick, '$', '')                  as nikhd
    , replace(gettv, '$', '')                 as gettv
    , replace(tvone, '$', '')                 as tvonehd
    , replace(aspire, '$', '')                as asprehd
    , replace(people, '$', '')                as peopltv
    , replace("bet her", '$', '')             as bherhd
    , replace(cheddar, '$', '')               as chedstr
    , replace(cooking, '$', '')               as cookhd
    , replace(history, '$', '')               as hisstr
    , replace(science, '$', '')               as scihd
    , replace("tv land", '$', '')             as tvland
    , replace(hallmark, '$', '')              as hallstr
    , replace(lifetime, '$', '')              as lifestr
    , replace(sundance, '$', '')              as sunstr
    , replace(discovery, '$', '')             as dschd
    , replace(paramount, '$', '')             as parhd
    , replace(tastemade, '$', '')             as taste
    , replace("bbc america", '$', '')         as bbcahd
    , replace("motor trend", '$', '')         as mthd
    , replace("cheddar news", '$', '')        as cbn
    , replace("law  & crime", '$', '')        as lcstr
    , replace("dest. america", '$', '')       as desthd
    , replace("aniimal planet", '$', '')      as aplhd
    , replace("bbc world news", '$', '')      as bbcnahd
    , replace("comedy central", '$', '')      as cchd
    , replace("discovery life", '$', '')      as dlchd
    , replace("hallmark drama", '$', '')      as halldr
    , replace("lifeime movies", '$', '')      as lmnstr
    , replace("travel channel", '$', '')      as travhd
    , replace("american heroes", '$', '')     as ahchd
    , replace("hallmark movies", '$', '')     as hmmstr
    , replace("discovery family", '$', '')    as dfchd
    , replace("investigation disco", '$', '') as idhd
  from ad_network

)

, renamed as (

  select
    ad_month
    -- transformation
    -- 2: convert to number, use try_cast_numeric to catch empty strings
    ,          {{- try_cast_numeric('AESTR', 'decimal', 'FM9G999G999D99') -}}       as aestr
    ,          {{- try_cast_numeric('AMCSTR', 'decimal', 'FM9G999G999D99') -}}      as amcstr
    ,          {{- try_cast_numeric('AXS', 'decimal', 'FM9G999G999D99') -}}         as axs
    ,          {{- try_cast_numeric('BETHD', 'decimal', 'FM9G999G999D99') -}}       as bethd
    ,          {{- try_cast_numeric('DIYHD', 'decimal', 'FM9G999G999D99') -}}       as diyhd
    ,          {{- try_cast_numeric('FYISTR', 'decimal', 'FM9G999G999D99') -}}      as fyistr
    ,          {{- try_cast_numeric('GACHD', 'decimal', 'FM9G999G999D99') -}}       as gachd
    ,          {{- try_cast_numeric('GSNHD', 'decimal', 'FM9G999G999D99') -}}       as gsnhd
    ,          {{- try_cast_numeric('IFCSTR', 'decimal', 'FM9G999G999D99') -}}      as ifcstr
    ,          {{- try_cast_numeric('MTVHD', 'decimal', 'FM9G999G999D99') -}}       as mtvhd
    ,          {{- try_cast_numeric('OWNHD', 'decimal', 'FM9G999G999D99') -}}       as ownhd
    ,          {{- try_cast_numeric('TLCHD', 'decimal', 'FM9G999G999D99') -}}       as tlchd
    ,          {{- try_cast_numeric('VH1HD', 'decimal', 'FM9G999G999D99') -}}       as vh1hd
    ,          {{- try_cast_numeric('CMTVHD', 'decimal', 'FM9G999G999D99') -}}      as cmtvhd
    ,          {{- try_cast_numeric('CLEO', 'decimal', 'FM9G999G999D99') -}}        as cleo
    ,          {{- try_cast_numeric('FOODHD', 'decimal', 'FM9G999G999D99') -}}      as foodhd
    ,          {{- try_cast_numeric('HGTVD', 'decimal', 'FM9G999G999D99') -}}       as hgtvd
    ,          {{- try_cast_numeric('INSPHD', 'decimal', 'FM9G999G999D99') -}}      as insphd
    ,          {{- try_cast_numeric('LOGOHD', 'decimal', 'FM9G999G999D99') -}}      as logohd
    ,          {{- try_cast_numeric('MTV2HD', 'decimal', 'FM9G999G999D99') -}}      as mtv2hd
    ,          {{- try_cast_numeric('UPHD', 'decimal', 'FM9G999G999D99') -}}        as uphd
    ,          {{- try_cast_numeric('VICESTR', 'decimal', 'FM9G999G999D99') -}}     as vicestr
    ,          {{- try_cast_numeric('NIKHD', 'decimal', 'FM9G999G999D99') -}}       as nikhd
    ,          {{- try_cast_numeric('GETTV', 'decimal', 'FM9G999G999D99') -}}       as gettv
    ,          {{- try_cast_numeric('TVONEHD', 'decimal', 'FM9G999G999D99') -}}           as tvonehd
    ,          {{- try_cast_numeric('ASPREHD', 'decimal', 'FM9G999G999D99') -}}           as asprehd
    ,          {{- try_cast_numeric('PEOPLTV', 'decimal', 'FM9G999G999D99') -}}           as peopltv
    ,          {{- try_cast_numeric('BHERHD', 'decimal', 'FM9G999G999D99') -}}      as bherhd
    ,          {{- try_cast_numeric('CHEDSTR', 'decimal', 'FM9G999G999D99') -}}           as chedstr
    ,          {{- try_cast_numeric('COOKHD', 'decimal', 'FM9G999G999D99') -}}      as cookhd
    ,          {{- try_cast_numeric('HISSTR', 'decimal', 'FM9G999G999D99') -}}      as hisstr
    ,          {{- try_cast_numeric('SCIHD', 'decimal', 'FM9G999G999D99') -}}       as scihd
    ,          {{- try_cast_numeric('TVLAND', 'decimal', 'FM9G999G999D99') -}}      as tvland
    ,          {{- try_cast_numeric('HALLSTR', 'decimal', 'FM9G999G999D99') -}}           as hallstr
    ,          {{- try_cast_numeric('LIFESTR', 'decimal', 'FM9G999G999D99') -}}           as lifestr
    ,          {{- try_cast_numeric('SUNSTR', 'decimal', 'FM9G999G999D99') -}}      as sunstr
    ,          {{- try_cast_numeric('DSCHD', 'decimal', 'FM9G999G999D99') -}}       as dschd
    ,          {{- try_cast_numeric('PARHD', 'decimal', 'FM9G999G999D99') -}}       as parhd
    ,          {{- try_cast_numeric('TASTE', 'decimal', 'FM9G999G999D99') -}}       as taste
    ,          {{- try_cast_numeric('BBCAHD', 'decimal', 'FM9G999G999D99') -}}      as bbcahd
    ,          {{- try_cast_numeric('MTHD', 'decimal', 'FM9G999G999D99') -}}        as mthd
    ,          {{- try_cast_numeric('CBN', 'decimal', 'FM9G999G999D99') -}}         as cbn
    ,          {{- try_cast_numeric('LCSTR', 'decimal', 'FM9G999G999D99') -}}       as lcstr
    ,          {{- try_cast_numeric('DESTHD', 'decimal', 'FM9G999G999D99') -}}      as desthd
    ,          {{- try_cast_numeric('APLHD', 'decimal', 'FM9G999G999D99') -}}       as aplhd
    ,          {{- try_cast_numeric('BBCNAHD', 'decimal', 'FM9G999G999D99') -}}           as bbcnahd
    ,          {{- try_cast_numeric('CCHD', 'decimal', 'FM9G999G999D99') -}}        as cchd
    ,          {{- try_cast_numeric('DLCHD', 'decimal', 'FM9G999G999D99') -}}       as dlchd
    ,          {{- try_cast_numeric('HALLDR', 'decimal', 'FM9G999G999D99') -}}      as halldr
    ,          {{- try_cast_numeric('LMNSTR', 'decimal', 'FM9G999G999D99') -}}      as lmnstr
    ,          {{- try_cast_numeric('TRAVHD', 'decimal', 'FM9G999G999D99') -}}      as travhd
    ,          {{- try_cast_numeric('AHCHD', 'decimal', 'FM9G999G999D99') -}}       as ahchd
    ,          {{- try_cast_numeric('HMMSTR', 'decimal', 'FM9G999G999D99') -}}      as hmmstr
    ,          {{- try_cast_numeric('DFCHD', 'decimal', 'FM9G999G999D99') -}}       as dfchd
    ,          {{- try_cast_numeric('IDHD', 'decimal', 'FM9G999G999D99') -}}        as idhd
  from remove_non_numeric

)

select * from renamed

