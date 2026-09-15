---
kernelspec:
  display_name: Python 3
  name: python3
---

# Configuring the `ESGFCatalog`

By default, the ESGFCatalog is configured to point at Globus-based indices with information about holdings at the OLCF (Oak Ridge Leadership Computing Facility) and ALCF (Argonne Leadership Computing Facility). To see all the configration defaults, we can print the `conf` object associated with the package.

```{code-cell} python
import intake_esgf
print(intake_esgf.conf)
```

The configuration state is global to the package and is printed here in `yaml`
format. You will notice a few keywords:

- `local_cache` - a list of paths to which we download data if found to be missing. We will download data to the first cache path to which you have write access.
- `esg_dataroot` - a list of paths that we will check for ESGF data prior to downloading. We have pre-populated this list with a few locations we know to check, but you may also add more if you are working on a resource with direct access to even a partial copy of the ESGF holdings.
- `logfile` - the full path to the location of the log file.
- `download_db` - the full path to the SQL database with download records.
- `globus_indices` - a dictionary whose keys are the Globus index name and values map to a boolean indicating that the index is enabled.
- `solr_indices` - a dictionary whose keys are the Solr index base url and values map to a boolean indicating that the index is enabled.
- `additional_df_cols` - a list of additional columns to include in the search results DataFrame. Columns that are not part of the search response will be ignored.
- `requests_cache` - a dictionary with arguments to [`requests_cache.session.CachedSession`](https://requests-cache.readthedocs.io/en/stable/user_guide.html#cachedsession). This is used to cache search results to speed up repeated searches.
- `confirm_download` - enable to require users to provide confirmation before any downloading occurs.

## Indices

As of this writing in Fall of 2026, ESGF is in a state of transition. Formerly, information about the datasets that may be downloaded as part of ESGF holdings were distributed around the world. The federation is restructuring and moving to storing ESGF dataset information (metadata and download locations) as part of [Spatiotemporal Asset Catalogs (STAC)](https://stacspec.org/en). In the future, you will be able to configure a single STAC index and be confident that you will receive all the information we have globally on any given dataset. In the interim or if you are looking for data from old projects, you may need to configure additional indices. We have written intake-esgf to understand all present and former index styles so it is simply a matter of enabling the appropriate index. The indices are organized by their underlying technology:

1. Solr: The former ESGF-1.0 index technology that is being phased out. We maintain a list of known index nodes inside our configuration variable `intake_esgf.conf['solr_indices']`. These will be disabled by default and may not return a response. If you know that one of these indices has been permamently turned off, please open an [issue](https://github.com/esgf2-us/intake-esgf/issues) and let us know and we will remove it from the list. We recommend enabling these as a last resort if no other option returns results.
2. Globus: As a temporary solution to allow the federation to retire the Solr technology, a US-based ESGF project migrated all Solr records from any US-based index node and put them in `intake_esgf.conf['globus_indices']`. This includes most older projects and a lot of `CMIP6` but not all of it. For this reason, it will be enabled by default.
3. STAC: At the time of this writing, ESGF has two STAC-based indices, enabled by default in `intake_esgf.conf['stac_indices']`. These indices should be identical but in the short term while CMIP7 publishing is ramping up, we recommend leaving them both enabled. These indices will contain `CMIP7` information and represent the future of all ESGF publishing. While the federation has not committed to republishing all old projects, they are actively working on republishing `CMIP6` records into these indices.

While many models have data records on more than one index, there is no single index that has information about all ESGF datasets for all projects. However, you will also find that some indices are slow to return a response and may not provide records that cannot be obtained elsewhere. For this reason, in `intake-esgf` indices may all be turned on and off as you desire. You will have to balance how long you are willing to wait with your need for complete information globally.

If for example, you wish to exclude the west (US-based) STAC index:

```{code-cell} python
from intake_esgf import ESGFCatalog

intake_esgf.conf.set(indices={"discovery.west.esgf.io":False})
cat = ESGFCatalog()
for ind in cat.indices:
    print(ind)
```

Note that we will only use your input dictionary to change the status of the indices you specify, leaving the others untouched. If you would like to ensure that you have all ESGF dataset information in your search, we provide a `all_indices` convenience keyword:

```{code-cell} python
intake_esgf.conf.set(all_indices=True)
cat = ESGFCatalog()
for ind in cat.indices:
    print(ind)
```

## Cache directories

The location to which we will download ESGF holdings is set to `${HOME}/.esgf` by default. However, you may change this location:

```{code-cell} python
intake_esgf.conf.set(local_cache="~/tmp")
print(intake_esgf.conf['local_cache'])
```

This can be particularly useful if you are working on a shared resource such as an institutional cluster or group workstation. On these machines, your home directory could have a limiting memory quota which can be avoided by pointing the cache directory to a shared project directory. This has the added benefit that others with read access to your project can use the data.

This package is designed to download data only if it is absolutely necesary. If you are working on a resource with direct access to some portion of the ESGF holdings, you can point to it and avoid downloading the data for which you already have direct access.

```{code-cell} python
intake_esgf.conf.set(esg_dataroot="/path/to/some/location")
print(intake_esgf.conf['esg_dataroot'])
```

Both of these configuration options completely replace the value already set. If you wish to postpend a directory to the existing set of locations, you may refer to the current option and simply add a value.

```{code-cell} python
intake_esgf.conf.set(local_cache=intake_esgf.conf["local_cache"] + ["~/another"])
print(intake_esgf.conf['local_cache'])
```

## Scope

Setting a configuration option using `set` will stay in effect as long as this session is active. That is, as long as you are working in a given script, ipython instance, or Jupyter notebook. If you were to open a new session, the configuration will return to the default.

It is worth noting that configuration options may be scoped in a block using the `with` statement:

```{code-cell} python
:tags: [remove-cell]
intake_esgf.conf.reset()
```

```{code-cell} python
with intake_esgf.conf.set(all_indices=True):
    cat = ESGFCatalog()
    print("catalog inside 'with'")
    for ind in cat.indices:
        print(ind)
print("\ncatalog after 'with'")
cat = ESGFCatalog()
for ind in cat.indices:
    print(ind)
```

If you would like to save your current configuration and make it the new default, you may call `save`.

```{code-cell} python
intake_esgf.conf.save()
```

```{code-cell} python
:tags: [remove-cell]
import os
from pathlib import Path
os.system(f"rm -f {Path.home() / '.config/intake-esgf/conf.yaml'}")
```
