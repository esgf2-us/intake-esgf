---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Simplifying Search with Model Groups


```{code-cell}
from intake_esgf import ESGFCatalog
cat = ESGFCatalog().search(
    experiment_id="historical",
    variable_id=["tas", "gpp"],
    frequency="mon",
)
print(cat)
```
