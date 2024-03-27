{%- macro user_is_billable(user) -%}
  (
    {{ user }}.roles is null
    or (
      {{ user }}.roles not like '%"super"%'
      and {{ user }}.roles not like '%"loadgen"%'
      and {{ user }}.roles not like '%"vip"%'
      and {{ user }}.roles not like '%"vip_premium"%'
      and {{ user }}.roles not like '%"unbilled"%'
      and {{ user }}.roles not like '%"test"%'
      and {{ user }}.roles not like '%_test"%'
      and {{ user }}.roles not like '%"qa_test_user"%'
      and {{ user }}.roles not like '%"philo_dot_com_user"%'
    )
  )
{%- endmacro -%}

{%- macro is_billable(role) -%}
  (
    {{ role }} is null
    or (
      {{ role }} not like '%"super"%'
      and {{ role }} not like '%"loadgen"%'
      and {{ role }} not like '%"vip"%'
      and {{ role }} not like '%"vip_premium"%'
      and {{ role }} not like '%"unbilled"%'
      and {{ role }} not like '%"test"%'
      and {{ role }} not like '%_test"%'
      and {{ role }} not like '%"qa_test_user"%'
      and {{ role }} not like '%"philo_dot_com_user"%'
    )
  )
{%- endmacro -%}
