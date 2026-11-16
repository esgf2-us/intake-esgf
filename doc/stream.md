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
import intake_esgf
```

In addition to the transfer of entire files, data may be streamed to the user as it is required by their script or application. The benefit is that if only a small portion of the data is to be used, we avoid downloading/storing the whole file. At the time of this writing, ESGF is actively publishing assets that enable streaming using 3 technologies.

- [Kerchunk](https://github.com/fsspec/kerchunk)/[VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr) reference files
- [CFA](https://github.com/cedadev/CFAPyX), netCDF files that are designed the following the CF-1.12 conventions for aggregations
- [OPENDAP](https://www.opendap.org), links which enable server-side subsetting of data

`intake-esgf` will harvest these assets/links from the index and return a dataset that is already configured to use them. To demonstrate this functionality, consider the following search for latent heat data from the `E3SM-2-1` model.

```{code-cell} python
:tags: [remove-output]
cat = ESGFCatalog().search(
    project="CMIP6Plus",
    experiment_id="historical",
    source_id="E3SM-2-1",
    variable_id="hfls",
    frequency="mon",
)
cat.remove_ensembles()
```

```{code-cell} python
:tags: [remove-input]
cat
```

In order to make use of streaming information, if present, simply pass `prefer_streaming=True` to the `to_dataset_dict()` function call. Not all files will have this capability, but if they do, then this will tell `intake-esgf` to use them following a priority order.

```{code-cell} python
:tags: [remove-output]
dsd = cat.to_dataset_dict(prefer_streaming=True,add_measures=False)
```

```{code-cell} python
:tags: [remove-input]
dsd
```

At this point, the dataset dictionary is returned but you will notice that no file download messages were received. Internally, `intake-esgf` used the streaming information we found and setup a xarray dataset that is ready to stream data. We now proceed with the analysis as if the data is local. In this example, we will plot a decadal mean over the continental United States.

```{code-cell} python
ds = dsd["hfls"]
ds = ds.sel(
    time=slice("2010-01-01", "2021-01-01"),
    lat=slice(24.5, 49.4),
    lon=slice(235.22, 293.05),
).mean(dim="time")
```

Now we can plot this trace using matlotlib. When the xarray dataset needs data, it just streams the required data.

```{code-cell} python
fig, ax = plt.subplots(figsize=(6, 3))
ds["hfls"].plot(ax=ax);
```

This can be a very fast alternative if the data volume is relatively low. If you want to verify that data has indeed been streamed and not accessed locally, you may print the session log and look at what was accessed.

```{code-cell} python
print(cat.session_log())
```

If you look towards the bottom of that log, you will see that a https link was accessed in place of a local file. Note that if a local file is present in your local cache, we will use that file even if you have preferred to use streaming.

## Configuration Settings

Internally, `intake-esgf` keeps a priority list for streaming technologies that are found for your datasets. If you choose to prefer streaming, then we will use the first link sorted by your technology preference and transfer speed of the hostname as recorded from your previous downloads. To see the priority list:

```{code-cell} python
print(intake_esgf.conf['streaming_preference'])
```

`intake-esgf` will only try to stream technologies that are found in this list. For example, if you never want to use OPENDAP, you may:

```{code-cell} python
intake_esgf.conf.set(streaming_preference=['Kerchunk','CFA'])
print(intake_esgf.conf['streaming_preference'])
```

You may want to always stream and avoid having to include the `prefer_streaming=True` in each call to `to_dataset_dict()`. To do this set the configure variable:

```{code-cell} python
intake_esgf.conf.set(prefer_streaming=True);
```

The configure value will only be used if no option is given in the `to_dataset_dict()` call.
