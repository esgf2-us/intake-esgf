
# Documentation for intake-esgf

```{image} _static/logo.png
:alt: ESGF2-US logo
:width: 40%
:align: left
```

`intake-esgf` is an `intake` and `intake-esm` *inspired* package under development in ESGF2. The data catalog is populated by querying a number of index nodes and puts together a global view of where the datasets may be found. If you are familiar with the interface for `intake-esm`, then using this package should be straightforward.

```{important}
ESGF is in the process of decomissioning the old index technology (Solr). While this process takes place, you may find some services may be broken or disappear completely. The information in the US-based Solr indices has been moved into a Globus (ElasticSearch) index which is this package's default index.
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
