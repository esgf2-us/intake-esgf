---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Configuring the `ESGFCatalog`

By default, the ESGFCatalog is configured to point at a Globus-based index (build on [Elasticsearch](https://www.elastic.co/)) with information about holdings at the ALCF (Argonne Leadership Computing Facility). This is a temporary default while we work on redesigning the index.

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

To do. Comment about working on shared resourece where you have a quota.

## Using data directly

To do.
