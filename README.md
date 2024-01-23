[<img width=250px src=./doc/_static/logo.png>](https://climatemodeling.science.energy.gov/presentations/esgf2-building-next-generation-earth-system-grid-federation)

# intake-esgf

## Badges

[![Continuous Integration][ci-badge]][rtd-link]
[![Documentation Status][rtd-badge]][rtd-link]

## Overview

`intake-esgf` is an [intake-esm](https://github.com/intake/intake-esm) *inspired* package under development in ESGF2. The main difference is that in place of querying a static index which is completely loaded at runtime, `intake-esgf` catalogs initialize empty and are populated by searching, querying ESGF index nodes.

## Installation

You may install `intake-esgf` using [pip](https://pypi.org/project/pip/):

```bash
python -m pip install intake-esgf
```

## Features

For a full listing of features with code examples, please consult the [documentation](https://intake-esgf.readthedocs.io/en/latest/?badge=latest). In brief, `intake-esgf` aims to hide some of the complexity of obtaining ESGF data and get the user the data as fast as we can.

* Indices are queried in parallel and report when they fail to return a response. The results are aggregated and presented to the user as a [pandas](https://pandas.pydata.org/) DataFrame.
* The locations of the data are hidden from the user. Internally we track which locations provide the user the fastest transfers and automatically favor them for you.
* Files are downloaded in parallel into a local cache which mirrors the remote storage directory structure. They are returned to the user as a dictionary of [xarray](https://xarray.dev/) Datasets. Your search script then becomes the way you download data as well as how you load it into memory for your analysis.
* Prior to downloading data, we first check that it is not already available locally. This could be because you had previously downloaded it, but also because you are working on a server that has direct access.
* Cell measure information is harvested from your search results and automatically included in the returned datasets.


[ci-badge]: https://github.com/esgf2-us/intake-esgf/actions/workflows/ci.yml/badge.svg?branch=main
[ci-link]: https://github.com/esgf2-us/intake-esgf/actions/workflows/ci.yml
[rtd-badge]: https://readthedocs.org/projects/intake-esgf/badge/?version=latest
[rtd-link]: https://intake-esgf.readthedocs.io/en/latest/?badge=latest
