---
geometry: "left=1.1in,right=1in,bottom=0.5in,top=0.8in"
pagestyle: "empty"
---
# Cheat Sheet
Full documentation: https://intake-esgf.readthedocs.io

## Basic Usage
```python
from intake_esgf import ESGFCatalog                          # import the basic object

cat = ESGFCatalog()                                      # initialize an empty catalog

cat.search(**keywords)                # populate the catalog by faceted keyword search

cat.to_dataset_dict(                        # download/load datasets into a dictionary
    ignore_facets = ['table_id']                  # don't use these facets in the keys
    minimal_keys = True,                           # disable to use all facets in keys
    separator = ".",                            # character between facets in the keys
    add_measures = True,                        # disable to skip adding cell measures
    prefer_streaming = False,                         # enable to stream when possible
    globus_endpoint = "UUID",      # specify a destination UUID to use Globus transfer
    globus_path = "path_to_cache",      # additional path for the destination endpoint
    quiet = False                                   # enable to suppress screen output
)

cat.to_path_dict()                # same as above but return paths instead of datasets
```
## Post-Search Helpers
```python
cat.model_groups()       # return dataset counts for each unique (model,grid,ensemble)

cat.remove_incomplete(f)      # remove incomplete groups as defined by user function f

cat.remove_ensembles()          # remove all but the smallest ensemble per model group
```
## Utility
```python
cat.from_tracking_ids(["hdl:21.14100/0..."])     # populate catalog using tracking IDs

cat.download_summary()                     # what have we got from where and how fast?

cat.session_log()                                       # what exactly did we just do?

cat.variable_info("text")                  # which variables are related to this text?
```
## Configure
```python
import intake_esgf                        # import the package to get access to config

intake_esgf.conf                                  # show the current configure options

intake_esgf.conf.set(                                             # change the options
    indices = {'index': True},                     # include/exclude the given indices
    all_indices = False,                               # enable to include all indices
    esg_dataroot = ["path_to_data"],      # the read-only paths where we look for data
    local_cache = ["path_to_cache"],     # the location(s) into which we download data
    break_on_error = True,   # disable to use whatever data we can successfully access
)

intake_esgf.conf.save()               # make the current configure options the default
```
