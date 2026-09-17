[<img width=250px src=./doc/_static/logo.png>](https://climatemodeling.science.energy.gov/presentations/esgf2-building-next-generation-earth-system-grid-federation)

# intake-esgf

## Badges

[![Continuous Integration][ci-badge]][ci-link]
[![Documentation Status][rtd-badge]][rtd-link]
[![Code Coverage Status][codecov-badge]][codecov-link]
[![PyPI][pypi-badge]][pypi-link]
[![Conda][conda-badge]][conda-link]
[![Zenodo][zenodo-badge]][zenodo-link]

## Announcement

There are two important ESGF changes to note as you use intake-esgf:

1. CMIP7 data is now available and therefore has become the default project. If you have old scripts that you wish to still be operational, you will need to add `project="CMIP6"` to your search.
2. ESGF has decomissioned the old index technology (Solr). However, some Solr indices may still be running and intake-esgf will still be able to interact with them, but they are not [configured](configure.md) by default. By default, we enable:
   - `ESGF2-US-1.5-Catalog`, a globus-based index with information about previous projects apart from CMIP7. This includes most anything from a previous US-based Solr index.
   - Two STAC-based indices (`discovery.east.esgf.io` and `discovery.west.esgf.io`). Long-term these indices will be identical and you will only want one of them enabled. While publishing (CMIP7 and some older projects) is ramping up, you may find they differ slightly and so we leave them both enabled for now.

## Overview

`intake-esgf` gives you programmatic access to ESGF holdings. We aim to hide some of the complexity of obtaining ESGF data and get it to the user as fast as we can.

[<img width=700px src=./doc/_static/basic.gif>](https://intake-esgf.readthedocs.io/en/latest/?badge=latest)

- Indices are queried in parallel and report when they fail to return a response. The results are aggregated and presented to the user as a [pandas](https://pandas.pydata.org/) DataFrame.
- The locations of the data are hidden from the user. Internally we track which locations provide the user the fastest transfers and automatically favor them for you.
- Files are downloaded in parallel into a local cache which mirrors the remote storage directory structure. They are returned to the user as a dictionary of [xarray](https://xarray.dev/) Datasets. Your search script then becomes the way you download data as well as how you load it into memory for your analysis.
- Prior to downloading data, we first check that it is not already available locally. This could be because you had previously downloaded it, but also because you are working on a server that has direct access.
- Cell measure information is harvested from your search results and automatically included in the returned datasets.
- The old (Solr) and new (STAC) index types are supported simultaneously.

For a full listing of features with code examples, please consult the [documentation](https://intake-esgf.readthedocs.io/en/latest/?badge=latest).

## Installation

You may install `intake-esgf` using [pip](https://pypi.org/project/pip/):

```bash
python -m pip install intake-esgf
```

or [conda-forge](https://conda-forge.org/):

```bash
conda install -c conda-forge intake-esgf
```

Once installed, you can verify installation with a 5 minute automated tutorial with:

```bash
python -m intake_esgf
```

[ci-badge]: https://github.com/esgf2-us/intake-esgf/actions/workflows/ci.yml/badge.svg?branch=main
[ci-link]: https://github.com/esgf2-us/intake-esgf/actions/workflows/ci.yml
[rtd-badge]: https://readthedocs.org/projects/intake-esgf/badge/?version=latest
[rtd-link]: https://intake-esgf.readthedocs.io/en/latest/?badge=latest
[codecov-badge]: https://img.shields.io/codecov/c/github/esgf2-us/intake-esgf.svg?logo=codecov
[codecov-link]: https://codecov.io/gh/esgf2-us/intake-esgf
[pypi-badge]: https://img.shields.io/pypi/v/intake-esgf?logo=pypi
[pypi-link]: https://pypi.org/project/intake-esgf
[conda-badge]: https://img.shields.io/conda/vn/conda-forge/intake-esgf?logo=anaconda
[conda-link]: https://anaconda.org/conda-forge/intake-esgf
[zenodo-badge]: https://zenodo.org/badge/691233416.svg
[zenodo-link]: https://zenodo.org/doi/10.5281/zenodo.11104809
