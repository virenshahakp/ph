{% docs dim_users %}

User profile record that is 1:1 with profiles. A single billable account
can have multiple profiles and thus multiple user profile records.

{% enddocs %}

{% docs account_id %}

The unique identifier for each billed account.

The id of the user who is the payer on the account also used as the account identifier.

{% enddocs %}

{% docs audience %}

An identifier for a user audience. This is expected to be unique across the audiences, with words separated by hyphens and no spaces so that it can
be easily used in a programatic fashion.

{% enddocs %}

{% docs audience_name %}

A label for a user audience. This is the descriptive label that can be placed on a report or in other documents to clearly indicate the composition of the audience.

{% enddocs %}

{% docs is_account_owner %}

A flag to indicate that this user profile is also the owner of the account. That is that the user_id and the account_id are the same unique identifier.

{% enddocs %}

{% docs user_id %}

The unique identifier to track a user profile.

The identifier for each distinct profile used to access philo content. There can be multiple user_ids related to each billable account_id. The profile that is used for billing is also used as the account_id. 

{% enddocs %}



