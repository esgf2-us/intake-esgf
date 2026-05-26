---
kernelspec:
  display_name: Python 3
  name: python3
---

# Streaming Data

```{code-cell} python
:tags: [remove-cell]
import matplotlib.pyplot as plt
from intake_esgf import ESGFCatalog
```

```{warning}
ESGF is in transition, moving to [STAC](https://stacspec.org/en/)-based catalogs as CMIP7 becomes available. It is our experience that some services have become fragile, especially OPeNDAP links. While `intake-esgf` will only return streaming links that have returned a successful response, we are seeing that even the returned links are not functional. You may find that even this tutorial fails to render with connection issues.
```

In addition to the transfer of entire files, data may be streamed to the user as it is required by their script. The benefit is that if only a small portion of the data is to be used, we avoid downloading the whole file. At the time of this writing, ESGF indices only contain [OPeNDAP](https://www.opendap.org/) access information. However, as we consider expanding support, the below interface will extend to other streaming/cloud-ready technologies such as [Zarr](https://zarr.dev/) stores, [kerchunk](https://github.com/fsspec/kerchunk), and [VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr).

To demonstrate this functionality, consider the following search for some future surface air temperature data from the UKESM model.

```{code-cell} python
:tags: [remove-output]
cat = ESGFCatalog().search(
    experiment_id="ssp585",
    source_id="UKESM1-0-LL",
    variable_id="tas",
    frequency="mon",
)
cat.remove_ensembles()
```
```{code-cell} python
:tags: [remove-input]
cat
```
To harvest the OPeNDAP access link from the index nodes, you tell the package that you `prefer_streaming=True`. Not all files will have this capability, but if they do, then this will tell `intake-esgf` to use them. Also, in this example we do not need any cell measures and so we will disable that in this search.

```{code-cell} python
:tags: [remove-output]
dsd = cat.to_dataset_dict(prefer_streaming=True, add_measures=False)
```
```{code-cell} python
:tags: [remove-input]
dsd
```


At this point, the dataset dictionary is returned but you will notice that no file download messages were received. The OPeNDAP access link was passed to the xarray constructor. We now proceed with our analysis as if the data is local. In this example, we wish to see what future temperatures will be under the SSP585 scenario over my hometown.

```{code-cell} python
ds = dsd["tas"]
ds = ds.sel(lat=35.96, lon=-83.92 + 360, method="nearest")
```

Now we can plot this trace using matlotlib. When the xarray dataset needs data, it uses the OPeNDAP protocol to stream just the time trace at the specific location.

```{code-cell} python
fig, ax = plt.subplots(figsize=(10, 3))
ds["tas"].plot(ax=ax);
```

This can be a very fast alternative if the data volume is relatively low. If you want to verify that data has indeed been streamed and not accessed locally, you may print the session log and look at what was accessed.

```{code-cell} python
print(cat.session_log())
```

If you look towards the bottom of that log, you will see that a https link was accessed in place of a local file. Note that if a local file is present in your local cache, we will use that file even if you have preferred to use streaming.
