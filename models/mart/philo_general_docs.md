{% docs hashed_session_id %}

An identifier that uniquely tracks a user session which can last an
indefinite amount of time. Each time a user signs in they get a new
hashed_session_id.

{% enddocs %}

{% docs playback_session_id %}

An identifier that tracks a unique playback session and represents 
a user initiating the playback of some asset in one continuous 
stretch of time. If a user is watching a live channel they can 
watch multiple assets in the same playback_session_id.

{% enddocs %}

{% docs asset_id %}

An identifier that uniquely represents a Philo video asset.

{% enddocs %}

{% docs position_start %}

The presentation time in seconds (0 being the start of an asset) where 
the user started watching an asset.

{% enddocs %}

{% docs position_stop %}

The presentation time in seconds (0 being the start of an asset) where 
the user ended watching an asset. Also the presentation time of the playhead
at the time of a sync event.

{% enddocs %}

{% docs platform %}

The Philo platform on which the event occurred or was requested.

{% enddocs %}

{% docs packages %}

The Philo content package that a subscriber has (or had) access to.

{% enddocs %}

{% docs subscriber_billing %}

The billing platform for the account.

{% enddocs %}

{% docs subscriber_state %}

The subscriber state as reported by Philo.

{% enddocs %}

{% docs is_wifi %}

True if the user's device is connected to a wifi network.

{% enddocs %}

{% docs is_cellular %}

True if the users' device is connected to a cellular network.

{% enddocs %}

{% docs app_version %}

The version of the Philo app that is running.

{% enddocs %}

{% docs analytics_version %}

A monotonically incrementing version identifier that is incremented
when a client makes significant changes to events that it fires.

{% enddocs %}

{% docs os_version %}

The version of the operating system that is running.

{% enddocs %}

{% docs screen_height %}

The height of the device's screen.

{% enddocs %}

{% docs screen_width %}

The width of the device's screen.

{% enddocs %}

{% docs client_ip %}

The public IP of the device that sent the event.

{% enddocs %}

{% docs device_name %}

The name of the device, often a marketing codename for device

{% enddocs %}

{% docs device_manufacturer %}

The manufacturer of the device.

{% enddocs %}

{% docs device_model %}

The model identifier of the device.

{% enddocs %}

{% docs as_number %}

The autonomous system number the client is using to connect
to the internet. This should uniquely identify the user's
ISP. The data comes from our GEO IP service and is based
on the user's IP address.

{% enddocs %}

{% docs as_name %}

The autonomous system name the client is using to connect
to the internet. This will be the name of the user's ISP.
The data comes from our GEO IP service and is based
on the user's IP address.

{% enddocs %}

{% docs geohash %}

The geohash based on lat/long returned by our GEO IP service.

{% enddocs %}

{% docs dma %}

The dma returned by our GEO IP service.

{% enddocs %}

{% docs show_id %}

The unique identifier of a show in our guide data.

{% enddocs %}

{% docs channel_id %}
The unique identifier of a channel in our guide data.
{% enddocs %}

{% docs channel_name %}
The name of the channel that aired the show.
{% enddocs %}

{% docs channel_callsign %}
The abbreviated name or callsign for the channel.
{% enddocs %}

{% docs show_title %}
The title of the show; note we bucket movies and specials into shows as well.
{% enddocs %}

{% docs episode_title %}
The title of the episode; this can be null if this is a movie or a special.
{% enddocs %}

{% docs episode_id %}
Unique identifier for an episode.
{% enddocs %}

{% docs run_time %}
The duration of the show in seconds.
{% enddocs %}

{% docs tms_series_id %}
A unique identifier for a series as provided by gracenote.
{% enddocs %}

{% docs philo_series_id %}
An identifer to group content, shows and episodes, that may have changed names or show ids,
but are to be considered as a group. We utilize the tms_series_id if it exists, else the show
or channel identifiers. This a string and the prefix of SERIES, SHOW, CHANNEL will indicate what
sort of grouping this series is. Note that CHANNEL series are likely not a true collection of content
but is more likely to reflect an inability to determine precisely what was being played.
{% enddocs %}

{% docs series_title %}
The first show title for the series. We use the first show id seen for each philo series id.
{% enddocs %}

{% docs show_episode_id %}
A md5 hash of the show id and episode id fields to uniquely identify each episode of content or movie.
{% enddocs %}

{% docs channel_episode_id %}
A md5 hash of the channel_id, show_id, and episode_id fields to uniquely identify each episode of content or movie per channel.
Any live content that has no show or episode content will share the hashed id of the channel as we are unable to identify that content further.
{% enddocs %}

{% docs dbt_processed_at %}
The time of the start of the redshift transaction where this record was processed. This is primarily used for incremental modeling.
{% enddocs %}

{% docs tile_group_id %}
b64 encoded tile group Id. 
{% enddocs %}

{% docs query_history %}
An incrementally updating table keeping a permanent history of the sys_query_history table. For more details on contents:
https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_HISTORY.html
{% enddocs %}

{% docs query_metrics %}
An incrementally updating table keeping a permanent history of the svl_query_metrics_summary table, enhanced with details from stl_query. For more details on contents:
https://docs.aws.amazon.com/redshift/latest/dg/r_SVL_QUERY_METRICS_SUMMARY.html
https://docs.aws.amazon.com/redshift/latest/dg/r_STL_QUERY.html
{% enddocs %}

{% docs stl_scan %}
An incrementally updating table keeping a permanent history of the stl_scan table. For more details on contents:
https://docs.aws.amazon.com/redshift/latest/dg/r_STL_SCAN.html
{% enddocs %}

{% docs table_info %}
An incrementally updating table keeping a permanent snapshot of table info from svv_table_info over time. For more details on contents:
https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html
{% enddocs %}

{% docs ungranted_locks %}
An incrementally updating table keeping a permanent snapshot of query locks on table leveraging various system tables. For more details on contents:
https://docs.aws.amazon.com/redshift/latest/dg/r_STV_LOCKS.html
https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TRANSACTIONS.html
{% enddocs %}