[<img width=250px src=https://nvcl.energy.gov/content/images/project/earth-system-grid-federation-2-93.jpg>](https://climatemodeling.science.energy.gov/presentations/esgf2-building-next-generation-earth-system-grid-federation)

# intake-esgf

## Badges

[![Continuous Integration][ci-badge]][rtd-link]
[![Documentation Status][rtd-badge]][rtd-link]

## Motivation
A small intake and intake-esm *inspired* package under development in ESGF2.
This package queries a sample index of the replicas hosted at Argonne National
Laboratory and returns the response as a pandas dataframe, mimicing the
interface developed by [intake-esm](https://github.com/intake/intake-esm). As a
user accesses ESGF data, this package will maintain a local cache of files
stored in `${HOME}/.esgf` as well as a log of searches and downloads in `${HOME}/.esgf/esgf.log`.

## Design Principles

* The user wants their data as fast as possible without needing to understand where it is coming from or how ESGF is organized.
* The search should concise enough that it becomes part of the analysis script and is also how data is loaded into memory.

## Overview

* While implemented to inform new developments in ESGF2, this package can also point to ESGF1 indices (`ESGFCatalog(esgf1_indices=True)` for all nodes or `ESGFCatalog(esgf1_indices=["esgf-node.llnl.gov"])` to pick a subset).
* When performing a search, we query all indices in parallel and merge the results in a pandas dataframe. The notion of which node the data lives on is transparent to the user.
* As in `intake-esm`, once the search describes the datasets that you want to use in your analysis, call `cat.to_dataset_dict()`. The package will then get file information from the indices and then either load the data from local holdings (previously downloaded or directly available) or download it in parallel. They keys of the returned dictionary of xarray datasets use the dataset id and the minimal set of faceets to uniquely describe each dataset being returned.
* If the script is run on resources where direct data access is available, you can set the path with `cat.set_esgf_data_root(...)` and then the package will prefer this location for loading data.  This makes your script portable and easily used in server-side computing.
* The package harvests `cell_measure` information from the dataset attributes and then automatically finds, downloads, and associates the appropriate measures with each dataset. As many times the measures are not present for each experiment/variant, we relax search criteria until the appropriate measure matching the `source_id`/`grid_label` is found.

## Future

* Currently the package will attempt to download files using the first https link that it finds. If a link fails, we continue on to the next link in the list. However, this list should be prioritized by what is fastest for the user. This is possibly something we can measure and adapt as the user uses the tool.
* We currently use the https links to download the data. However, we plan to add a `stream=True` option to `to_dataset_dict` which would not download but rather pass OPeNDAP/THREDDS links to the xarray constructor.
* A growing number of file entries now also contain Globus links. We will add authentication and then the option to select and endpoint to download the current catalog to.

[ci-badge]: https://github.com/esgf2-us/intake-esgf/actions/workflows/ci.yml/badge.svg?branch=main
[ci-link]: https://github.com/esgf2-us/intake-esgf/actions/workflows/ci.yml
[rtd-badge]: https://readthedocs.org/projects/intake-esm/badge/?version=latest
[rtd-link]: https://intake-esm.readthedocs.io/en/latest/?badge=latest
