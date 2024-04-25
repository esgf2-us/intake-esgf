---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Logging

If you would like details about what `intake-esgf` is doing, look in the local cache directory (the default location is `${HOME}/.esgf/`) for a `esgf.log` file. This is a full history of everything we have searched, downloaded, or accessed.

You can also look at just this session (since you instantiated the catalog) by calling `session_log()` and printing it. Consider the following search.

```{code-cell}
import intake_esgf
from intake_esgf import ESGFCatalog

with intake_esgf.conf.set(all_indices=True):
    cat = ESGFCatalog().search(
        source_id="IPSL-CM6A-LR",
        experiment_id="piControl",
        variable_id="areacella",
        variant_label="r1i1p1f1",
        frequency="fx",
    )
    ds = cat.to_dataset_dict(add_measures=False)
    print(cat.session_log())
```

In this case you will see how long each index took to return a response and if any failed as well as from where the file was downloaded if not already on your system. Initially we randomize download locations from all available, but as you use `intake-esgf` we will remember the hosts which provide you the fastest download times. You can see where your data has come from by:

```{code-cell}
cat.download_summary()
```

We use this database to prioritize download locations internally to get you data as fast as we can.
