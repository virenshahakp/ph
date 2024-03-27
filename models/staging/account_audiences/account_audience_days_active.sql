-- dynamic audiences built off of stream starts for last 1, 7, 14, 28 days

with

users as (

  select * from {{ ref('dim_users') }}

)

, accounts as (

  select * from {{ ref('dim_accounts') }}

)

, stream_starts as (

  select distinct
    user_id
    , event_date
  from {{ ref('fct_stream_starts') }}
  -- restrict this to only the last 28 completed days for performance
  where event_timestamp between current_date - 29 and current_date

)

, account_stream_start_days as (

  select
    stream_starts.*
    , users.account_id
  from stream_starts
  join users on (stream_starts.user_id = users.user_id)
  join accounts on (users.account_id = accounts.account_id)
  where accounts.is_billable is true

)

, active_in_recent_days as (

  {% for days in (1,7,14,28) %}

    {% if loop.first == false %}
      union all
    {% endif %}

    /*
    we could also generate user level audiences with similar logic
    we have chosen to generate at the account level for now.

    SELECT
      user_id
      , 'user-active-last-{{ days }}-days' AS audience
      , 'All Users Active within the Previous {{ days }} Days' AS audience_name
    FROM account_stream_start_days
    WHERE event_date BETWEEN CURRENT_DATE - {{ days }} AND CURRENT_DATE

    UNION ALL
    */


    select distinct
      account_id
      , 'account-active-last-{{ days }}-days'                     as audience
      , 'All Accounts Active within the Previous {{ days }} Days' as audience_name
    from account_stream_start_days
    where event_date between current_date - {{ days }} and current_date

    {% if days > 1 %}

      union all

      select --distinct
        account_id
        , 'account-active-all-{{ days }}-days'                        as audience
        , 'All Accounts Active Every Day in Previous {{ days }} Days' as audience_name
      from account_stream_start_days
      where event_date between current_date - {{ days }} and current_date
      {{ dbt_utils.group_by(n=3) }}
      having count(distinct event_date) = {{ days }}

    {% endif %}

  {% endfor %}

)

select * from active_in_recent_days
