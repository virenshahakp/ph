# ads documentation

{% docs fct_ad_pods %}

Ad pods inserted into the player feed.
Is inclusive of all inserted pods
regardless if the player reached and
rendered them. This is particularly
important if working with VOD data as
pods are inserted at stream start
rather than just prior to the pod's
position in the feed. For viewed ad
pods, see beacons.

{% enddocs %}

{% docs fct_beacons %}

all ads that were rendered in the
player feed. This is a proxy for ad
viewed as we cannot be sure that the
user was in front of the device when
the ad was displayed.

{% enddocs %}


{% docs fct_vast_ads_usage %}

fct_vast_ads_enriched aggregated to day/hour level
and enriched with demographic and geographic
data (where available). Empowers quicker
data visualization and segment slicing in
dashboards. 

{% enddocs %}

{% docs dim_demand_partner_map %}

All bidder names, ids, and their subsequent
demand partner groupings.
dim_demand_partner_map is the lookup tbl
for how revenue and impressions from those
demand partners and bidders should be
classified.

{% enddocs %}