# Documentation for intake-esgf

```{image} _static/logo.png
:alt: ESGF2-US logo
:width: 40%
:align: left
```

`intake-esgf` is an `intake` and `intake-esm` _inspired_ package under development in ESGF2. The data catalog is populated by querying a number of index nodes and puts together a global view of where the datasets may be found. If you are familiar with the interface for `intake-esm`, then using this package should be straightforward.

```{important}
There are two important ESGF changes to note as you use intake-esgf:
1. CMIP7 data is now available and therefore has become the default project. If you have old scripts that you wish to still be operational, you will need to add `project="CMIP6"` to your search.
2. ESGF has decomissioned the old index technology (Solr). However, some Solr indices may still be running and intake-esgf will still be able to interact with them, but they are not [configured](configure.md) by default.
This documentation will still contain code samples focused on CMIP6, but as more data is published they will transition to CMIP7.
```

## Installation

`intake-esgf` can be installed using `pip`:

```bash
pip install intake-esgf
```

or through `conda-forge`

```bash
conda install -c conda-forge intake-esgf
```
