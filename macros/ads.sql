{%- macro advertising_contract_types() -%}
  case
    when 
      asset_type != 'vod' 
      and channel in ('ae', 'ci', 'fyi', 'history', 'lifetime', 'lmn', 'viceland') 
    then 'a&e'

    when 
      asset_type != 'vod'
      and channel in ('gsn') 
    then 'gsn'

    when 
      asset_type != 'vod' 
      and channel in (
        'lawcrime'
        , 'bloomberg'
        , 'cheddar'
        , 'gac'
        , 'gacliving'
        , 'gettv'
        , 'gusto'
        , 'people'
        , 'playerstv'
        , 'retrocrush'
        , 'revry'
        , 'decades'
        , 'heroes'
        , 'metv'
        , 'story'
        , 'cowboyway'
        , 'usatoday'
        , 'dragraceuniverse'
      ) 
    then 'rev share long break'
    
    when 
      asset_type != 'vod' 
      and channel in (
        'amc'
        , 'bbca'
        , 'ifc'
        , 'sundance'
        , 'wetv'
        , 'pocketwatch'
        , 'ryanandfriends'
        , 'tastemade'
        , 'bet'
        , 'cmtv'
        , 'comedy'
        , 'logo'
        , 'mtv'
        , 'mtv2'
        , 'nik'
        , 'paramount'
        , 'tvland'
        , 'vh1'
        , 'bobross' 
        , 'comedydynamics'
        , 'screambox'
        , 'failarmy'
        , 'peopleareawesome'
        , 'thepetcollective'
        , 'outside'
      ) 
    then 'inventory share'

    when 
      asset_type != 'vod' 
      and channel in (
        'accuweather'
        , 'aspire'
        , 'axs'
        , 'hdnet'
        , 'ahc'
        , 'animalplanet'
        , 'cooking'
        , 'destinationamerica'
        , 'discovery'
        , 'discoveryfamily'
        , 'discoverylife'
        , 'food'
        , 'hgtv'
        , 'id'
        , 'magnolia'
        , 'motortrend'
        , 'own'
        , 'sci'
        , 'travel'
        , 'tlc'
        , 'g4'
        , 'sony'
        , 'hallmark'
        , 'food'
        , 'hmm'
        , 'insp'
        , 'reelz'
        , 'aspire'
        , 'cleo'
        , 'tvone'
        , 'bether'
        , 'starttv'
        , 'fetv'
        , 'fmc'
        , 'poptv'
        , 'smithsonian'
      ) 
    then 'traditional'

    when 
      asset_type = 'vod' 
    then 'vod'

    else 'other'
  end
{%- endmacro -%}

{% macro is_guaranteed_unpaid(source = "vast_ads", cpm_field = "cpm_price") %}
(
  {{ source }}.{{ cpm_field }}::numeric = 1
  and {{ source }}.ad_system = 'directcampaign'
)
{% endmacro %}

{% macro cpm_price(source = "vast_ads", cpm_field = "cpm_price") %}

(case 
  when {{ source }}.{{ cpm_field }}::float <= 1 
    then 0.0
  else {{ source }}.{{ cpm_field }}::float
end)

{% endmacro %}

{% macro is_viable(source = "vast_ads", version_field = "manifest_system_version") %}

(case
  when {{ source }}.{{ version_field }} is not null --stitcher
    then
      coalesce(
        {{ source }}.is_fingerprint_dup is not true
          and {{ source }}.is_ingested is true
          and {{ source }}.is_fcapped is false
          and {{ source }}.is_url_dup is false
          and {{ source }}.is_dup is false
          and {{ source }}.has_duration_mismatch is false
          and {{ source }}.is_active is true
        , false
      )
  when {{ source }}.{{ version_field }} is null --sutured
    then
      coalesce(
        {{ source }}.is_fingerprint_dup is not true
          and {{ source }}.is_url_dup is false
          and {{ source }}.is_ingested is true
          and {{ source }}.is_dup is false
        , false
      )
end)

{% endmacro %}

{% macro pod_instance_id(
  source
  , pod_id = "pod_id"
  , player_id = "player_id"
  , request_id = "request_id"
  ) 
%}

  coalesce({{ source }}.{{ pod_id }}, 'pod_id_error') || 
    ':' || 
    coalesce({{ source }}.{{ player_id }}, 'player_id_error') || 
    ':' || 
    coalesce({{ source }}.{{ request_id }}, 'request_id_error')

{% endmacro %}

{% macro monetizable_space() %}
  CASE
      when network = 'accuweather' and channel = 'accuweather' and owner in ('distributor') then TRUE
      when network = 'aetv' and channel = 'ae' and owner in ('distributor', 'provider') then TRUE
      when network = 'aetv' and channel = 'ci' and owner in ('distributor') then TRUE
      when network = 'aetv' and channel = 'fyi' and owner in ('distributor', 'provider') then TRUE
      when network = 'aetv' and channel = 'history' and owner in ('distributor', 'provider') then TRUE
      when network = 'aetv' and channel = 'lifetime' and owner in ('distributor', 'provider') then TRUE
      when network = 'aetv' and channel = 'lmn' and owner in ('distributor', 'provider') then TRUE
      when network = 'aetv' and channel = 'viceland' and owner in ('distributor', 'provider') then TRUE
      when network = 'aspire' and channel = 'aspire' and owner in ('distributor') then TRUE
      when network = 'aspire' and channel = 'up' and owner in ('distributor') then TRUE
      when network = 'axs' and channel = 'axs' and owner in ('distributor') then TRUE
      when network = 'axs' and channel = 'hdnet' and owner in ('distributor') then TRUE
      when network = 'cinedigm' and channel = 'bobross' and owner in ('distributor') then TRUE
      when network = 'cinedigm' and channel = 'screambox' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'ahc' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'animalplanet' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'destinationamerica' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'discovery' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'discoveryfamily' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'discoverylife' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'id' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'motortrend' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'own' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'sci' and owner in ('distributor') then TRUE
      when network = 'discovery' and channel = 'tlc' and owner in ('distributor') then TRUE
      when network = 'fetv' and channel = 'fetv' and owner in ('distributor') then TRUE
      when network = 'fetv' and channel = 'fmc' and owner in ('distributor') then TRUE
      when network = 'gac' and channel = 'gac' and owner in ('distributor', 'provider') then TRUE
      when network = 'gac' and channel = 'gacliving' and owner in ('distributor', 'provider') then TRUE
      when network = 'gsn' and channel = 'gsn' and owner in ('distributor') then TRUE
      when network = 'hallmark' and channel = 'hallmark' and owner in ('distributor') then TRUE
      when network = 'hallmark' and channel = 'hallmarkdrama' and owner in ('distributor') then TRUE
      when network = 'hallmark' and channel = 'hmm' and owner in ('distributor') then TRUE
      when network = 'hubbard' and channel = 'reelz' and owner in ('distributor') then TRUE
      when network = 'insp' and channel = 'insp' and owner in ('distributor') then TRUE
      when network = 'revolt' and channel = 'revolt' and owner in ('distributor') then TRUE
      when network = 'scripps' and channel = 'cooking' and owner in ('distributor') then TRUE
      when network = 'scripps' and channel = 'food' and owner in ('distributor') then TRUE
      when network = 'scripps' and channel = 'hgtv' and owner in ('distributor') then TRUE
      when network = 'scripps' and channel = 'magnolia' and owner in ('distributor') then TRUE
      when network = 'scripps' and channel = 'travel' and owner in ('distributor') then TRUE
      when network = 'sony' and channel = 'sony' and owner in ('distributor') then TRUE
      when network = 'urbanone' and channel = 'cleo' and owner in ('distributor') then TRUE
      when network = 'urbanone' and channel = 'tvone' and owner in ('distributor') then TRUE
      when network = 'viacom' and channel = 'bether' and owner in ('distributor') then TRUE
      when network = 'viacom' and channel = 'poptv' and owner in ('distributor') then TRUE
      when network = 'viacom' and channel = 'smithsonian' and owner in ('distributor') then TRUE
      when network = 'weigel' and channel = 'starttv' and owner in ('distributor') then TRUE
      else FALSE
    end
{% endmacro %}