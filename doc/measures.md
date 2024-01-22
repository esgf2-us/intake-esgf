---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Automatic Cell Measures

If you have worked with CMIP data before, you know that cell measure information
like `areacella` is needed to take proper area-weighted means/summations. Yet
many times, model centers have not uploaded this information uniformly in all
submissions. This can be frustrating for the user.

In `intake-esgf`, when you call `to_dataset_dict()`, we perform a search for
each dataset being placed in the dataset dictionary, progressively dropping
facets to find, if possible, the cell measures that are *closest* to the dataset
being downloaded. Sometimes they are simply in another `variant_label`, but
other times they could be in a different `activity_id`. No matter where they
are, we find them for you and add them to your dataset by default (disable with
`add_measures=False`).

Consider the following search for data with `UKESM1-0-LL`. We are looking for a land variable `gpp`, the gross primary productivity.

```{code-cell}
from intake_esgf import ESGFCatalog
cat = ESGFCatalog().search(
    variable_id="gpp",
    source_id="UKESM1-0-LL",
    variant_label="r2i1p1f2",
    frequency="mon",
    experiment_id="historical",
)
dsd = cat.to_dataset_dict()
```

The progress bar will let you know that we are searching for cell measure
information. We determine which measures need downloaded by looking in the
dataset attributes. Since `gpp` is a land variable, we see that its
`cell_measures ='area: areacella'` which indicates that this data should be also
downloaded. However you will also find `where land` in the `cell_methods`
meaning that we also need `sftlf`, the land fractions. If you look at the
resulting dataset, you will find that both have been associated.

```{code-cell}
dsd["gpp"]
```

What makes this particular example difficult is that the cell measures for this model are only found in the `piControl` experiment, for the `r1i1p1f2` variant. Our methods finds the right measures, which you can see by printing out the session log and looking for which `areacella` files are downloaded / accessed.

```{code-cell}
print(cat.session_log())
```
