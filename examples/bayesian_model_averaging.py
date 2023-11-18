from multiprocessing.pool import ThreadPool

import intake
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from drs import drs
from tqdm import tqdm

from intake_esgf import ESGFCatalog
from intake_esgf.base import bar_format

# Using the ILAMB CRU4.02 as a reference dataset
ilamb = intake.open_catalog(
    "https://raw.githubusercontent.com/nocollier/intake-ilamb/main/ilamb.yaml"
)
ref = ilamb["tas | CRU4.02"].read()

# Define the models that we will use
cat = ESGFCatalog().search(
    source_id=["CanESM5", "INM-CM5-0", "CESM2"],
    experiment_id="historical",
    variable_id="tas",
    table_id="Amon",
)
cat.remove_ensembles()
dss = cat.to_dataset_dict(ignore_facets=["institution_id", "grid_label"])

# Prepare the data for efficient comparisons. We need to trim the time away and make
# sure that all models are on lon \in [0,360] with the 0 at the Prime Meridian. We also
# will reindex the time domain to match the reference and interpolate to the reference
# resolution.
ref = ref.sel(time=slice("1980-01-01", "2015-01-01"))
ref["lon"] = xr.where(ref["lon"] < 0, ref["lon"] + 360, ref["lon"])
ref = ref.sortby(["time", "lat", "lon"])
for model in dss:
    dss[model] = dss[model].sel(time=slice("1980-01-01", "2015-01-01"))
    dss[model]["time"] = ref["time"]
    dss[model] = dss[model].interp(lat=ref.lat, lon=ref.lon, method="nearest")
    dss[model] = dss[model].drop_vars(
        ["time_bnds", "time_bounds", "lat_bnds", "lon_bnds"], errors="ignore"
    )
    dss[model].load()  # <-- speeds up computation but costs memory


# Define the likelihood as the area- and time-weighted RMSE
def likelihood(ds):
    rmse = np.sqrt(
        (
            ((ds["tas"] - ref["tas"]) ** 2)
            .weighted(ds["areacella"].fillna(0))
            .mean(dim=["lat", "lon"])
        )
        .weighted(ref["time_bounds"].diff(dim="nb").astype(float) * 1e-9 / 24 / 3600)
        .mean()
    )
    return rmse.values


# Generate a vector uniform distribution using the Dirichlet-Rescale (DRS) algorithm
# implemented in https://github.com/dgdguk/drs.
n_samples = 1000
models = list(dss.keys())
n_models = len(models)
prior = np.array(
    [
        drs(n_models, 1.0, np.ones(n_models), np.zeros(n_models))
        for i in range(n_samples)
    ]
)
prior = {model: prior[:, i] for i, model in enumerate(models)}


# Define the function that maps sample_index --> rmse
def compute_sample(i):
    mean = sum([dss[model] * prior[model][i] for model in models])
    return likelihood(mean)


# Compute the posterior distribution in parallel using threads.
num_threads = 4
posterior = [
    post
    for post in tqdm(
        ThreadPool(num_threads).imap_unordered(compute_sample, range(n_samples)),
        total=n_samples,
        bar_format=bar_format,
        unit="sample",
        unit_scale=False,
        desc="Computing posterior",
        ascii=True,
    )
]

# Find the mean weights among the 5% best posterior values
best_percentile = 5.0
best_indices = np.where(posterior < np.percentile(posterior, best_percentile))
weights = {model: p[best_indices].mean() for model, p in prior.items()}

# Make plots of the model distributions
fig, axs = plt.subplots(
    figsize=(10, n_models * 3), nrows=n_models, ncols=2, tight_layout=True
)
for i, m in enumerate(models):
    axs[i, 0].hist(prior[m])
    axs[i, 0].set_title(f"Prior {m}")
    axs[i, 0].set_xlim(0, 1)
    axs[i, 1].hist(prior[m][best_indices])
    axs[i, 1].set_title(f"Posterior {m}")
    axs[i, 1].set_xlim(0, 1)
fig.savefig("distributions.png")
plt.close()
