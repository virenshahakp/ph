{% docs fct_user_signed_up_all_sources %}

  This table contains a row for each user_id and each anonymous_id the user has used. 
  Each row has the timestamp that we first saw the user.

  We want to know when a user_id first signed up. Currently, this is the first time 
  that we have identified the user or the when the user has created an account. 
  The min identify timestamp is the first time we mapped an anonymous_id to a user_id 
  thus the closest time at which a user created an account.
  
  We also want to know what anonymous_ids each user has used. We take the 
  distinct user_id and signed_up_at timestamp and join them with all anonymous ids 
  the user has used. Thus this table will have multiple entries for each user_id.
  
  Related fct_account_signed_up_all_sources. 

{% enddocs %}

{% docs fct_account_signed_up_all_sources %}

  This is a subset of fct_user_signed_up_all_sources.

  This builds off of that model by excluding profiles other than the initial
  user, by ensuring that the user_id is the root_user_id on the account.

  This has multiple entries per account as it includes all anonymous_ids
  for the account. As this is a database view on top of the 
  fct_user_signed_up_all_sources we don't test the values here as they are tested
  in fct_user_signed_up_all_sources.

{% enddocs %}

{% docs fct_acquisition_funnel %}

  All user events in the acquisition funnel. 
  We can use this to track users as they come down the funnel from visit to paid. 
  Each entry will have info about the user, where the user is coming from, 
  the ip and other details about the user, and the time stamp of each activity.

{% enddocs %}
