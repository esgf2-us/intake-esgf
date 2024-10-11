---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

```{code-cell}
:tags: [remove-cell]
from intake_esgf import ESGFCatalog
from pprint import pprint
```

# Just the Paths

While the basic paradigm of `intake-esgf` is to return xarray datasets for everything in your catalog, we recognize that you may wish to just get the paths back and use them in creative ways.

1. You may not want to use xarray datasets. We highly recommend learning the package and using it in your research, but you may have alternatives and we do not want to prohibit you from working as you see fit.
2. The analysis script you are running may not have been written to leverage xarray datasets.
3. You may need just the paths to pass into another tool or benchmark package.
4. You may have specific options you want to pass to `xarray.open_dataset()` that our interface does not support.

There is a catalog method we call `to_path_dict()`. This works just like `to_dataset_dict()` except we do not call xarray dataset constructors on the paths returned for you. Both functions even have most of the same keyword arguments. If we perform a search

```{code-cell}
cat = ESGFCatalog().search(
    experiment_id="historical",
    source_id="CanESM5",
    frequency="mon",
    variable_id=["gpp", "tas", "pr"],
    member_id="r1i1p1f1",
)
```

Then we can call instead the path function and then print the local paths.

```{code-cell}
paths = cat.to_path_dict()
pprint(paths)
```

Note that this will also check first to see if data is available locally and download if not just as with `to_dataset_dict()`. In fact, internally our `to_dataset_dict()` function calls `to_path_dict()` first. You can also use this to obtain the OPenDAP links if you prefer.

```{code-cell}
cat = ESGFCatalog().search(
    experiment_id="historical",
    source_id="CanESM5",
    frequency="mon",
    variable_id=["cSoil"],
    member_id="r1i1p1f1",
)
paths = cat.to_path_dict(prefer_streaming=True)
pprint(paths)
```
