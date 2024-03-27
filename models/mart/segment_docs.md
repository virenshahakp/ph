{% docs anonymous_id %}

The unique identifier for this user and device as assigned by segment.

{% enddocs %}

{% docs context_ip %}

IP of the device used to report this event.

{% enddocs %}

{% docs context_campaign_content %}
<!---EM: could use a second set of eyes on these campaign content definitions--->
Google anayltics: Used to differentiate similar content, or links within the same ad. For example, if you have two call-to-action links within the same email message, you can use utm_content and set different values for each so you can tell which version is more effective

{% enddocs %}

{% docs context_campaign_content_id %}

The asset id for the deeplinked content that brought the user to launch the app.

{% enddocs %}

{% docs context_campaign_medium %}

The advertising or marketing medium, for example: cpc, banner, email newsletter.

{% enddocs %}

{% docs context_campaign_name %}
   
<!---MISSING DEFINITION. --->

{% enddocs %}

{% docs context_campaign_source %}

Identify the advertiser, site, publication, etc. that is sending traffic to your property, for example: google, newsletter4, billboard.

{% enddocs %}

{% docs context_page_referrer %}

Referrer for the current page in the browser. This is automatically collected by Segment's Analytics.js.

{% enddocs %}

{% docs context_page_path %}

Path for the current browser page. This is automatically collected by Segment's Analytics.js.

{% enddocs %}

{% docs context_term %}

<!---MISSING DEFINITION. Is this utm_term? why is the name context_term instead of context_campaign_term? --->

{% enddocs %}

{% docs context_page_url %}

The full URL for the current browser page. This is automatically collected by Segment's Analytics.js.

{% enddocs %}

{% docs context_user_agent %}

User agent of the device that made the request.

{% enddocs %}

{% docs event_id %}

The segment generated unique id for an analytics event.

{% enddocs %}

{% docs event_timestamp %}

The reported time from the client (potentially skewed) of when the stream start was requested.

{% enddocs %}

{% docs timestamp %}

The reported time from the client (potentially skewed) of when the stream start was requested.

{% enddocs %}

{% docs received_at %}

When the event message was received by segment.

{% enddocs %}

{% docs loaded_at %}

When the record was loaded into the data warehouse

{% enddocs %}

{% docs uuid_ts %}

When segment loaded the record into the data warehouse (typically aliased into the generic name 'loaded_at')

{% enddocs %}

{% docs environment_analytics_version %}

A version number that tracks updates to the fields collected for analytics purposes.

{% enddocs %}

{% docs environment_os_version %}

The version number of the operating system running in the device.

{% enddocs %}

{% docs environment_app_version %}

The version of the Philo app running of the device.

{% enddocs %}