---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Logging

If you would like details about what `intake-esgf` is doing, you can enable
debug logging. This requires a few extra lines of code to set up Python logging
and set the log level to "DEBUG".

```{code-cell}
import logging
from io import StringIO
from intake_esgf import ESGFCatalog

# Configure the Python logger to capture debug messages from intake-esgf
debug_log = StringIO()
logging.basicConfig(stream=debug_log, format="\033[36m%(asctime)s \033[0m%(message)s")
logging.getLogger("intake-esgf").setLevel(logging.DEBUG)

# Perform the search and download as usual
cat = ESGFCatalog().search(
    source_id="IPSL-CM6A-LR",
    experiment_id="piControl",
    variable_id="areacella",
    variant_label="r1i1p1f1",
    frequency="fx",
)
ds = cat.to_dataset_dict(add_measures=False)

# View the log
print(debug_log.getvalue())
```

In this case you will see how long each index took to return a response and if any failed as well as from where the file was downloaded if not already on your system. Initially we randomize download locations from all available, but as you use `intake-esgf` we will remember the hosts which provide you the fastest download times. You can see where your data has come from by:

```{code-cell}
cat.download_summary()
```

We use this database to prioritize download locations internally to get you data as fast as we can.
