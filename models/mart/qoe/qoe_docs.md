{% docs adapted_bitrate %}

The playback bitrate reported from a Philo platform. This is the bitrate that the player selected based on the the adaptive
bitrate algorithm.

{% enddocs %}

{% docs estimated_bandwidth %}

The bandwidth that the player estimates the user has available to
play video. This is used as an input to the adaptive bitrate algorithm.
Note that not all players give us access to this data.

{% enddocs %}

{% docs user_selected_bitrate %}

The bitrate that the user selected via a UI-based bitrate selector.
Users might select a bitrate to reduce bandwidth usage on their
device. Note that not all platforms have UI support for this feature.

{% enddocs %}

{% docs error_code %}

A platform-specific error code.

{% enddocs %}

{% docs error_description %}

A platform-specific error description.

{% enddocs %}

{% docs sutured_pid %}

An identifier that represents a unique timeline (ads + assets) used
by our manifest generator. Note that we cache these timelines, and re-use them if a user plays the same asset multiple times within a
asset-type-specific TTL.

This identifier is used when correlating with CDN and Sutured logs.

{% enddocs %}

{% docs is_new_session %}

True if this is the first session where a sutured_pid is used. False
if we are re-using a cached session.

{% enddocs %}

{% docs manifest_environment %}

The "track" used to serve the manifests for this session. Examples are
`manifest-kenny`, `manifest-staging2`, or `manifest` (prod).

{% enddocs %}

{% docs content_cdn_host %}

The host used to serve content for this session. Note that we never
switch CDNs in the middle of session, so this is immutable within a
playback session. Example: `prod.cdn-vdms.philo.com`.

{% enddocs %}

{% docs is_sender %}

True if this session represents a cross-device sender controlling
a receiver. Sender sessions will not have any client-fired events.

{% enddocs %}

{% docs played_asset_id %}

The asset_id that was played for the session. This will be either
a recording, a VOD, or a channel.

{% enddocs %}

{% docs session_created_at %}

the timestamp the session was created

{% enddocs %}

{% docs error_philo_code %}

player failure error code displayed to the user.
[MAIN Error codes](https://www.notion.so/philoinc/2c068bd743664faab3a1490b1861a5ca?v=215da8a0d8c94734982ca46cfbe50f99)
[Error Codes and Descriptions](https://www.notion.so/philoinc/b38ca62129c540bb8f5273fed741497d?v=bc8c3d7bcb064b67b7a9c8b1b3c518f0)

{% enddocs %}

{% docs error_detailed_name %}

Error message for `error_philo_code`

{% enddocs %}

{% docs error_http_status_code %}

http status code for the error

{% enddocs %}
