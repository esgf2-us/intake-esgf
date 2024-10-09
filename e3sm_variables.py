import pandas as pd
import xarray as xr

ds = xr.merge(
    [
        xr.open_dataset(fname)
        for fname in [
            "CBGCv1.LR.BGC-LNDATM.20TR.clm2.h0.1870-01.nc",
            "CBGCv1.LR.BGC-LNDATM.20TR.clm2.h2.1870-01.nc",
            "CBGCv1.LR.BGC-LNDATM.20TR.clm2.h3.1870-01.nc",
        ]
    ],
    compat="override",
)

df = []
for var, da in ds.items():
    row = {
        "variable": var,
        "long_name": da.attrs["long_name"] if "long_name" in da.attrs else "",
        "units": da.attrs["units"] if "units" in da.attrs else "",
    }
    row.update({dim: True for dim in da.dims})
    df.append(row)
df = pd.DataFrame(df)
df = df.fillna(False)
df = df[df.time & df.lndgrid][["variable", "long_name", "units"]].sort_values(
    "variable"
)
