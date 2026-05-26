---
kernelspec:
  display_name: Python 3
  name: python3
---

# Logging

If you would like details about what `intake-esgf` is doing, you can consult your catalog's session log. This will be everything we have done since your catalog was instantiated. Consider the following search.

```{code-cell} python
:tags: [remove-output]
from intake_esgf import ESGFCatalog

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
```{code-cell} python
:tags: [remove-input]
print(cat.session_log())
```

In this case you will see how long each index took to return a response and if any failed as well as from where the file was downloaded if not already on your system. Initially we randomize download locations from all available, but as you use `intake-esgf` we will remember the hosts which provide you the fastest download times. You can see where your data has come from by:

```{code-cell} python
cat.download_summary()
```

We use this database to prioritize download locations internally to get you data as fast as we can.
