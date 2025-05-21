---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Filter Files by Date

The intake-esgf paradigm is meant to somewhat hide from you the notion of
*files*. You search for *datasets* and the related files are loaded and
aggregated into a dictionary of xarray Datasets. However, many times your
intended use of any given dataset is only part of the whole time series. While
there is not a consistent convention throughout ESGF, many centers will save the
variables in separate files spanning portions of the total time span. Thus you
may end up downloading many files that you will never touch.

In order to avoid downloading all the files, when searching you may specify
`file_start` and/or `file_end`. Internally, we will save these time stamps and
when obtaining file information for loading/downloading, only touch files that
fall within some part of that span. If, for example, we only wish to analyze
data from the 60's through the 90's, we can add the following to our search:

```{code-cell}
from intake_esgf import ESGFCatalog
cat = ESGFCatalog().search(
    experiment_id="historical",
    variable_id="msftmz",
    source_id="NorESM2-LM",
    variant_label="r2i1p1f1",
    file_start="1960-01",
    file_end="1999-12"
)
dsd = cat.to_dataset_dict()
```

The NorESM2-LM model tends to heavily split up their model output, in this case
by decades. Notice the time span listed when we print the resulting dataset:

```{code-cell}
print(dsd["msftmz"])
```

Note that you do not need to provide both timestamps. If you want to check what
intake-esgf filtered, we write these out in the session log:

```{code-cell}
print(cat.session_log())
```
