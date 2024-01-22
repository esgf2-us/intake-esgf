"""A collection of common operators used in CMIP analysis."""
from typing import Union

import pandas as pd
import xarray as xr

from intake_esgf import IN_NOTEBOOK
from intake_esgf.base import bar_format, get_cell_measure, get_search_criteria

if IN_NOTEBOOK:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm


def global_sum(
    dsd: Union[dict[str, xr.Dataset], xr.Dataset], quiet: bool = False
) -> Union[dict[str, xr.Dataset], xr.Dataset]:
    """Integrate the datasets globally in place with the proper cell measures.

    Parameters
    ----------
    dsd
        The dataset or dictionary of datasets to integrate.

    """

    def _global_sum(ds: xr.Dataset):
        ds_sum = {}
        ds_attrs = ds.attrs
        for var, da in ds.items():
            measure = get_cell_measure(var, ds)
            if measure is None:
                continue
            attrs = da.attrs  # attributes get dropped, so we rem them
            da = (da * measure).sum(dim=measure.dims)
            da.attrs = attrs
            da.attrs["units"] = f"({da.attrs['units']}) * ({measure.attrs['units']})"
            ds_sum[var] = da
        ds_sum = xr.Dataset(ds_sum)
        ds_sum.attrs = ds_attrs
        return ds_sum

    if isinstance(dsd, xr.Dataset):
        return _global_sum(dsd)
    for key, ds in tqdm(
        dsd.items(),
        disable=quiet,
        bar_format=bar_format,
        unit="dataset",
        unit_scale=False,
        desc="Global sum",
        ascii=False,
        total=len(dsd),
    ):
        dsd[key] = _global_sum(ds)
    return dsd


def global_mean(
    dsd: Union[dict[str, xr.Dataset], xr.Dataset], quiet: bool = False
) -> Union[dict[str, xr.Dataset], xr.Dataset]:
    """Compute a area-weighted global mean of the datasets in place.

    Parameters
    ----------
    dsd
        The dataset or dictionary of datasets to average.

    """

    def _global_mean(ds: xr.Dataset):
        ds_mean = {}
        ds_attrs = ds.attrs
        for var, da in ds.items():
            measure = get_cell_measure(var, ds)
            if measure is None:
                continue
            attrs = da.attrs  # attributes get dropped, so we rem them
            da = da.weighted(measure.fillna(0)).mean(dim=measure.dims)
            da.attrs = attrs
            ds_mean[var] = da
        ds_mean = xr.Dataset(ds_mean)
        ds_mean.attrs = ds_attrs
        return ds_mean

    if isinstance(dsd, xr.Dataset):
        return _global_mean(dsd)
    for key, ds in tqdm(
        dsd.items(),
        disable=quiet,
        bar_format=bar_format,
        unit="dataset",
        unit_scale=False,
        desc="Global mean",
        ascii=False,
        total=len(dsd),
    ):
        dsd[key] = _global_mean(ds)
    return dsd


def ensemble_mean(
    dsd: dict[str, xr.Dataset], include_std: bool = False, quiet: bool = False
) -> dict[str, xr.Dataset]:
    """Compute the ensemble mean of the input dictionary of datasets.

    This routine intelligently combines the input data across where the only difference
    in the facets is the `variant_label`.

    Parameters
    ----------
    dsd
        The dictionary of datasets.
    include_std
        Enable to include the standard deviation in the output.
    quiet
        Enable to silence the progress bar.

    """
    # parse facets out of the dataset attributes
    df = []
    for key, ds in dsd.items():
        df.append(get_search_criteria(ds))
        df[-1]["key"] = key
    df = pd.DataFrame(df)
    if "version" in df.columns:
        df = df.drop(columns="version")
    # now groupby everything but the variant_label and compute the mean/std
    grp_cols = [c for c in list(df.columns) if c not in ["variant_label", "key"]]
    out = {}
    for _, grp in tqdm(
        df.groupby(grp_cols),
        disable=quiet,
        bar_format=bar_format,
        unit="dataset",
        unit_scale=False,
        desc="Ensemble mean",
        ascii=False,
    ):
        ds = xr.concat([dsd[key] for key in grp["key"].to_list()], dim="variant")
        ds.attrs["variant_label"] = grp["variant_label"].to_list()
        row = grp.iloc[0]
        out[row["key"].replace(row["variant_label"], "mean")] = ds.mean(
            dim="variant", keep_attrs=True
        )
        if include_std:
            out[row["key"].replace(row["variant_label"], "std")] = ds.std(
                dim="variant", keep_attrs=True
            )
    return out
