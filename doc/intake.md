# Why intake?

This package bears the name *intake* because of 2 python packages that are in use in the broader data and Earth systeming community:

- [intake](https://github.com/intake/intake): a lightweight package for finding, investigating, loading and disseminating data.
- [intake-esm](https://github.com/intake/intake-esm): an intake plugin for parsing an Earth System Model (ESM) catalog and loading assets into xarray datasets.

And while we certainly follow in their footsteps, you may notice that this package is not an intake plugin nor does it depend on either.

## intake

Formal intake catalogs are [meant](https://intake.readthedocs.io/en/latest/scope2.html#motivation) to describe the data that is available and free you from worrying about how to obtain it or get it into memory, no matter the storage format. This paradigm leaves you with concise data access code that is descriptive enough that it can be left in scripts.

In the case of ESGF data, static intake catalogs do not make sense. We have millions of datasets making the static catalogs far too large to be practical. Furthermore, ESGF data is already described in great detail in databases that can be queried using a web [interface](https://esgf-node.ornl.gov/search) or a [RESTful API](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html). Even the intake documentation provides the following reason to not use intake: *all your data needs are already met by some other data service by itself, for example a set of tables/procedures/views/queries on a SQL database.*

However, while it may not make sense to create formal, static intake catalogs for all of the ESGF data, the simplicity of data access is a feature we would like to provide ESGF users.

## intake-esm

[intake-esm](https://github.com/intake/intake-esm) was created as an intake plugin that provides static indices for *portions* of the ESGF holdings that are hosted outside of the federation. The community has come up with many use [cases](https://intake-esm.readthedocs.io/en/stable/reference/faq.html#is-there-a-list-of-existing-catalogs) for these types of catalogs:

- Modeling centers downloaded large portions of the ESGF archive to their own storage systems and wanted a way to let their users discover these datasets.
- Portions of the archive were converted to Zarr format and hosted on Google Cloud.
- Modeling centers ran non-CMIP experiments and wanted a method to let users discover and access these datasets.

These catalogs and the intake-esm software was created to address these and other use cases for privately developed catalogs.

In the process, intake-esm made strides in a vastly improved user interface. Users of ESGF data are accustomed to a web-based facetted search with results returned as lists of datasets in a web page that may be browsed and included in a data cart. In intake-esm, the catalogs may be searched in the same way but with results returned as a pandas dataframe (the python standard for representing tabular data). Data download/access is then handled automatically with a simple call to `to_dataset_dict()`.

## intake-esgf

We found this paradigm to be powerful and thus intake-esgf was born out of a desire to provide the same interface to ESGF users with a few important differences.

- Our catalogs initialize empty and are populated *dynamically* by searching a configurable list of indices.
- When searching for file information, we hide the data location, form a composite view of all access methods, and select the fastest option for the user.
- Data transfers only occur if necessary. Before making a transfer we check that the data does not already exist in a cache or other local source or that streaming was not preferred and available. This makes supporting data lakes simple and provides a method to accommodate other data access options in a single interface.

So while we do not explicitly depend on intake or intake-esm, our package is heavily influenced by both of these works. We have decided to keep the name `intake-esgf` despite the potential confusion as an homage to the packages that inspired this work.
