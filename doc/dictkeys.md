---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Output Dictionary Key Format

You have a lot of control over how you want they keys of the output dictionary to appear. For demonstration purposes, consider the following search.

```{code-cell}
from intake_esgf import ESGFCatalog
cat = ESGFCatalog().search(
    experiment_id="historical",
    variant_label="r1i1p1f1",
    frequency="mon",
    source_id=["CESM2", "CanESM5"],
    variable_id=["tas", "gpp"],
)
print(cat)
```

By default, we will build keys out of the facet values that are different among the entries in the output dictionary. So since all the datasets are in the same activity, experiment and use the same variant and grid labels, these facets need not be in the output dictionary keys.

```{code-cell}
ds = cat.to_dataset_dict()
for key in ds.keys():
    print(key)
```

## Ignoring Some Facets

However, on inspection you will notice that the institution and table are not needed either, but because they have different values were included in the keys by default. You can specify that certain facets be ignored in the output dictionary keys.

```{code-cell}
ds = cat.to_dataset_dict(ignore_facets=["institution_id", "table_id"])
for key in ds.keys():
    print(key)
```

## Use All Facets

You may decide that you do not like our attempt to provide simpler keys in which case you may use the full set of facets.

```{code-cell}
ds = cat.to_dataset_dict(minimal_keys=False)
for key in ds.keys():
    print(key)
```

## Change the Separator

You may also use a different separator. By default use the `.` symbol, but you may choose any character. This can be useful if you wish to use `xarray-datatree` to pass into their `DataTree` contructor.

```{code-cell}
ds = cat.to_dataset_dict(minimal_keys=False,separator="/")
for key in ds.keys():
    print(key)
```
