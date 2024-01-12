---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Attribution of Climate-Driven Carbon Cycle Anomalies

In this example, we present an analysis that illustrates how changes in future
climate anomalies (temperature, precipitation, and soil moisture) control an
omalies in the carbon cycle as measured by gross primary productivity. We will
restrict the anlysis to the monthly output from a single model for both the
historical period as well as the aggressive SSP5-8.5 scenario.

```{code-cell}
from intake_esgf import ESGFCatalog

cat = ESGFCatalog().search(
    variable_id=["gpp", "tas", "pr", "mrso"],
    experiment_id=["historical", "ssp585", "ssp370", "ssp245", "ssp126"],
    frequency="mon",
)
```
