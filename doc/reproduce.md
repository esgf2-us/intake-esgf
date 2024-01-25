---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

```{code-cell}
---
tags: [remove-cell]
---
from intake_esgf import ESGFCatalog
```

# Reproducibility

If you are using ESGF data in an analysis publication, the journal to which you
are submitting may require that you provide data citations or availability.
While we are working on improving this in ESGF, we also wanted to highlight the
current functionality. Consider the following query assumed to be used in an
unspecified analysis. For comparison, we will print the underlying dataframe to
show the results of the search.

```{code-cell}
cat = ESGFCatalog().search(
    experiment_id="historical",
    source_id="CanESM5",
    variable_id=["gpp", "tas", "nbp"],
    variant_label=["r1i1p1f1"],
    frequency="mon",
)
cat.df
```

In the course of the analysis, you would download the datasets into a dictionary.

```{code-cell}
dsd = cat.to_dataset_dict(add_measures=False)
```

Then you may loop through the datasets and pull out the `tracking_id` from the
global attributes of each dataset.

```{code-cell}
tracking_ids = [ds.tracking_id for _,ds in dsd.items()]
for tracking_id in tracking_ids:
    print(tracking_id)
```

The `tracking_id` is similar to a digital object identifier (DOI) and can be
provided in some form in your paper or supplemental material to be precise about
what ESGF data you used. If you have a list of `tracking_id`s, then you can pass
them into `from_tracking_ids()` to reproduce the catalog.

```{code-cell}
new_cat = ESGFCatalog().from_tracking_ids(tracking_ids)
new_cat.df
```

If you visually compare `cat` with `new_cat` you will see that they are the
same. From here you may interact with the new catalog and recover the data you
used if needed. This can also be used to quickly communicate the colleagues
which data should be used.
