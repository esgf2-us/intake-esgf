
# Frequently Asked Questions

#### Why aren't the results I get with intake-esgf the same as Metagrid?

As of this writing (November 2024) intake-esgf is configured to, by default, use Globus/ElasticSearch-based indices we are developing which contain a lot of the global ESGF holdings but not all of them. We provide backward compatibility to the old Solr-based nodes which Metagrid is using. You just need to [configure](configure) intake-esgf to use them.

Note that another reason for asymmetrical results is that an index may have failed to return a response. In intake-esgf, we print a warning to the screen when this happens, but Metagrid will report results with no warning. This can give you the impression that an error has occurred when really it is due to undependable indices. The federation is currently working hard to improve this situation in time for CMIP7.
