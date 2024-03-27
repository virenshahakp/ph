with

pages as (

  select * from {{ ref('web_prod_pages_source') }}

)

, campaigns as (

  select
    event_id
    , anonymous_id
    , user_id
    , context_ip
    , app_version
    , context_campaign_source
    , context_campaign_name
    , context_campaign_term
    , context_campaign_medium
    , context_page_referrer
    , context_user_agent
    , context_user_agent_id
    , context_page_path
    , context_campaign_content
    , url
    , visited_at
    , loaded_at
    , screen_name
    , coupon_code
    /* this regex looks for the 'contentid=' in the url and pulls out the value
    after the =.  Marketing set the contentid param without the utm_ prefix, so segment
    did not pull the value out.  This was corrected around the week of 10/15/2020.
    */
    , case
      when context_campaign_content_id is not null
        then context_campaign_content_id
      when url ilike '%contentid=%'
        then split_part(regexp_substr(url, 'contentid=[^&]*'), '=', 2)
    end as context_campaign_content_id

    /*
      manage all the campaign specific logic here
      take care that broad matches to a URL need to have specific
      logic to prioritize campaigns vs each other when multiple
      can match a given path
    */
    , case
      when url ilike 'https://www.philo.com%'
        and context_campaign_source = 'referral'
        and context_campaign_term = 'philo-701'
        then 1
      when url ilike 'https://try.philo.com%'
        then
          case
            when nullif(context_campaign_source, '') is null
              and nullif(context_campaign_term, '') is null
              then 3
            else 1
          end
      when url ilike 'https://www.philo.com/go%'
        then
          case
            when nullif(context_campaign_source, '') is null
              and nullif(context_campaign_term, '') is null
              then 3
            else 1
          end
      when url ilike 'https://www.philo.com/login/subscribe%'
        then
          case
            when nullif(context_campaign_source, '') is null
              and nullif(context_campaign_term, '') is null
              then 3
            else 1
          end
      when url ilike 'https://www.philo.com/login/signup%'
        then 3
      /*
      We add utm parameters to share links; specifically, we set campaign_name to
      'share', campaign_source to empty, and campaign_term to the share resources id.

      Shares are higher in priority than organic because a user is following a
      directed link to find out about Philo. There is not paid attribution thus it is
      not in priority 1.
      */
      when url ilike 'https://www.philo.com/%'
        and user_id is null
        and nullif(context_campaign_source, '') is null
        and context_campaign_name = 'share'
        then 2
      /* about.philo.com/landingpages */
      when url ilike 'https://about.philo.com%'
        then
          case
            when nullif(context_campaign_source, '') is null
              and nullif(context_campaign_term, '') is null
              then 3
            else 1
          end
      /* help.philo.com/landingpages */
      when url ilike 'https://help.philo.com%'
        then
          case
            when nullif(context_campaign_source, '') is null
              and nullif(context_campaign_term, '') is null
              then 3
            else 1
          end
      /* for canonical links to public show pages */
      when user_id is null
        and nullif(context_campaign_source, '') is not null
        and nullif(context_campaign_term, '') is not null
        then 3

    end as priority
    , case
      when url like 'https://www.philo.com%'
        and context_campaign_source = 'referral'
        and context_campaign_term = 'philo-701'
        then 'referral'
      when url ilike 'https://try.philo.com%'
        then
          case
            when context_page_referrer ilike '%go.philo.com%'
              then 'go-page'
            else 'try'
          end
      when url ilike 'https://www.philo.com/go%'
        then 'go-page'
      when url ilike 'https://www.philo.com/login/subscribe%'
        then 'direct_signup'
      when url ilike 'https://www.philo.com/login/signup%'
        then
          case
            when context_page_referrer ilike '%go.philo.com%'
              then 'go-page'
            else 'try'
          end
      when url ilike 'https://www.philo.com/%'
        and user_id is null
        and nullif(context_campaign_source, '') is null
        and context_campaign_name = 'share'
        then 'share'
        /* about.philo.com/landingpages */
      when url ilike 'https://about.philo.com%'
        then 'about'
        /* help.philo.com/landingpages */
      when url ilike 'https://help.philo.com%'
        then 'help'
        /* for canonical links to public show pages */
      when user_id is null
        and nullif(context_campaign_source, '') is not null
        then 'public-page'

    end as visit_type
    /* this regex looks for the 'ref=' in the url and pulls out the value
      after the =.  This value is set by nginx when a user visit philo.tv or another
      url that redirects to try.philo.com
      */
    , case
      when url ilike '%ref=%'
        then split_part(regexp_substr(url, 'ref=[^&]*'), '=', 2)
    end as reference
  from pages

)

select * from campaigns
