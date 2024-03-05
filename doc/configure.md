---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Configuring the `ESGFCatalog`

By default, the ESGFCatalog is configured to point at Globus-based indices (build on [Elasticsearch](https://www.elastic.co/)) with information about holdings at the OLCF (Oak Ridge Leadership Computing Facility) and ALCF (Argonne Leadership Computing Facility). This is a temporary default while we work on redesigning the index.

```{code-cell}
from intake_esgf import ESGFCatalog
cat = ESGFCatalog()
for ind in cat.indices: # Which indices are included?
    print(ind)
```

## ESGF1 Solr-based indices

While not the default, we have provided backwards compatibility with the ESGF1 Solr-based indices. When you instantiate a catalog, you can provide additional nodes using the keyword argument `esgf1_indices`. You may provide a single node, a list of nodes, or even `True` to use all of the nodes.

```{code-cell}
cat = ESGFCatalog(esgf1_indices="esgf-node.llnl.gov")  # include LLNL
cat = ESGFCatalog(esgf1_indices=["esgf-node.ornl.gov", "esgf.ceda.ac.uk"])  # ORNL & CEDA
cat = ESGFCatalog(esgf1_indices=True)  # all federated indices
for ind in cat.indices:
    print(ind)
```

`intake-esgf` is designed to build a composite response from all indices that you provide while also warning you if a node failed to return a response. If you want to ensure that your search has found all possible results, we recommend using `esgf1_indices=True`.

## Setting the local cache

The location to which we will download ESGF holdings is set to `${HOME}/.esgf` by default. However, you may change this location by a call to `cat.set_local_cache_directory()`. This can be particularly useful if you are working on a shared resource such as an institutional cluster or group workstation. On these machines, your home directory could have a limiting memory quota which can be avoided by pointing the cache directory to a shared project directory. This has the added benefit that others with read access to your project can use the data. Note that at this time, you will need to set the local cache in your analysis scripts before downloading/loading any data.

## Using data directly

This package is designed to download data only if it is absolutely necesary. If you are working on a resource with direct access to some portion of the ESGF holdings, you can point to it with `cat.set_esgf_data_root()`. This will add a read-only location to check for data. We check for a few locations automatically when the package is instantiated. If you would like a location added to our [defaults](https://github.com/esgf2-us/intake-esgf/blob/76fdb8e943f73813160bd76544d5d471c25f2a2d/intake_esgf/base.py#L169), please feel free to submit a [issue](https://github.com/esgf2-us/intake-esgf/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).
