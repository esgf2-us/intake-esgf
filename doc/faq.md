---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Frequently Asked Questions

###### Why aren't the results I get with intake-esgf the same as Metagrid?

As of this writing (November 2024) intake-esgf is configured to, by default, use Globus/ElasticSearch-based indices we are developing which contain a lot of the global ESGF holdings but not all of them. We provide backward compatibility to the old Solr-based nodes which Metagrid is using. You just need to [configure](configure) intake-esgf to use them.

Note that another reason for asymmetrical results is that an index may have failed to return a response. In intake-esgf, we print a warning to the screen when this happens, but Metagrid will report results with no warning. This can give you the impression that an error has occurred when really it is due to undependable indices. The federation is currently working hard to improve this situation in time for CMIP7.

###### Are your catalogs CSV/JSON files?

No, intake-esgf catalogs are initially empty and are populated only when you perform a faceted search by communicating with remote index nodes. The intake-esgf package can be [configured](configure) to dynamically query a number of ESGF-developed index nodes. At the moment these include some Globus/ElasticSearch-based indices as well as the old Solr-based technologies.

###### What projects does intake-esgf support?

You can check the projects which are currently supported:

```{code-cell}
from intake_esgf import supported_projects
print(supported_projects())
```

In principle, intake-esgf can be made to support any project for which we have index data. It is a matter of implementing some boilerplate code that tells intake-esgf how certain facets behave (model names, ensemble members, etc.). If you have a need, raise an [issue](https://github.com/esgf2-us/intake-esgf/issues).
