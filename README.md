[<img width=250px src=https://nvcl.energy.gov/content/images/project/earth-system-grid-federation-2-93.jpg>](https://climatemodeling.science.energy.gov/presentations/esgf2-building-next-generation-earth-system-grid-federation)

*Experimental version under development*

# intake-esgf

A small intake and intake-esm *inspired* package under development in ESGF2.
This package queries a sample index of the replicas hosted at Argonne National
Laboratory and returns the response as a pandas dataframe, mimicing the
interface developed by [intake-esm](https://github.com/intake/intake-esm). As a
user accesses ESGF data, this package will maintain a local cache of files
stored in `${HOME}/.esgf` as well as a log of searches and downloads.

# Example

The ESGF catalog initializes to nothing and needs populated with an initial
search. Under the hood, we use the globus sdk to query the index and the response
is parsed into a pandas dataframe for viewing.

```bash
>>> from intake_esgf import ESGFCatalog
>>> cat = ESGFCatalog()
>>> cat.search(
        experiment_id=["historical","ssp585"],
        source_id="CESM2",
        variable_id="tas",
        table_id="Amon",
    )
mip_era                                                     [CMIP6]
activity_id                      [ScenarioMIP, C4MIP, ISMIP6, CMIP]
institution_id                                               [NCAR]
source_id          [CESM2, CESM2-WACCM, CESM2-FV2, CESM2-WACCM-FV2]
experiment_id     [ssp585, esm-ssp585, ssp585-withism, historica...
member_id         [r11i1p1f1, r10i1p1f1, r4i1p1f1, r5i1p1f1, r3i...
table_id                                                     [Amon]
variable_id                                                   [tas]
grid_label                                                     [gn]
dtype: object
```

Notice that the result contains variants of `CESM2` and `ssp585`. This is
because by default the search is permissive to allow for flexible queries in
case the user is not sure for what they are searching. Repeating the search with
`strict=True` will remove the variants.

```bash
>>> cat.search(
        strict=True,
        experiment_id=["historical","ssp585"],
        source_id="CESM2",
        variable_id="tas",
        table_id="Amon",
    )
mip_era                                                     [CMIP6]
activity_id                                     [CMIP, ScenarioMIP]
institution_id                                               [NCAR]
source_id                                                   [CESM2]
experiment_id                                  [historical, ssp585]
member_id         [r9i1p1f1, r1i1p1f1, r5i1p1f1, r8i1p1f1, r11i1...
table_id                                                     [Amon]
variable_id                                                   [tas]
grid_label
dtype: object
```

Leaving `strict=False` can be useful if you are not sure what variable you need,
but have an idea of what it is called. The follow search reveals that there are
several choices for `variable_id` that have 'temperature' in the long name.

```bash
>>> cat.search(variable_long_name='temperature')
Displaying summary info for 1000 out of 480459 results:
mip_era                                                     [CMIP6]
activity_id       [ScenarioMIP, RFMIP, DCPP, DAMIP, CMIP, HighRe...
institution_id    [MPI-M, DKRZ, MOHC, CSIRO, CMCC, CCCma, CNRM-C...
source_id         [MPI-ESM1-2-LR, UKESM1-0-LL, ACCESS-ESM1-5, Ha...
experiment_id     [ssp370, ssp119, ssp245, ssp126, ssp585, ssp43...
member_id         [r10i1p1f1, r3i1p1f1, r9i1p1f1, r8i1p1f1, r2i1...
table_id          [CFmon, Emon, AERmonZ, Eday, Amon, 6hrPlevPt, ...
variable_id                           [ta, ts, ta500, ta850, ta700]
grid_label                             [gn, gnz, gr, gr1, grz, gr2]
dtype: object
```

Once you have reduced your query down to the files of interest, then call `to_dataset_dict()` which will download the files if needed or load them from available resources.

```bash
>>> cat = ESGFCatalog().search(
        strict=True,
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        member_id=["r1i1p1f1"],
)
ds = cat.to_dataset_dict()
```

If you are on a resource that has direct access to the ESGF data, you may set the path and then we use the paths from the search response to locate the datasets directly. For example, on OLCF systems with proper group access, the data is temporarily available.

```bash
>>> cat = ESGFCatalog().search(
        strict=True,
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        member_id=["r1i1p1f1"],
)
cat.set_esgf_data_root("/gpfs/alpine/cli137/proj-shared/ESGF/esg_dataroot/css03_data/")
ds = cat.to_dataset_dict()
```
