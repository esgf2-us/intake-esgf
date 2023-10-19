from typing import Union

import xarray as xr

from intake_esgf.util import get_cell_measure


def global_sum(
    dsd: Union[dict[str, xr.Dataset], xr.Dataset]
) -> Union[dict[str, xr.Dataset], xr.Dataset]:
    """Integrate the datasets globally in place with the proper cell measures.

    Parameters
    ----------
    dsd
        The dataset or dictionary of datasets to integrate.

    """

    def _global_sum(ds: xr.Dataset):
        ds_sum = {}
        for var, da in ds.items():
            measure = get_cell_measure(var, ds)
            if measure is None:
                continue
            attrs = da.attrs  # attributes get dropped, so we rem them
            da = (da * measure).sum(dim=measure.dims)
            da.attrs = attrs
            da.attrs["units"] = f"({da.attrs['units']}) * ({measure.attrs['units']})"
            ds_sum[var] = da
        return xr.Dataset(ds_sum)

    if isinstance(dsd, xr.Dataset):
        return _global_sum(dsd)
    for key, ds in dsd.items():
        dsd[key] = _global_sum(ds)
    return dsd


def global_mean(
    dsd: Union[dict[str, xr.Dataset], xr.Dataset]
) -> Union[dict[str, xr.Dataset], xr.Dataset]:
    """Compute a area-weighted global mean of the datasets in place.

    Parameters
    ----------
    dsd
        The dataset or dictionary of datasets to average.

    """

    def _global_mean(ds: xr.Dataset):
        ds_sum = {}
        for var, da in ds.items():
            measure = get_cell_measure(var, ds)
            if measure is None:
                continue
            attrs = da.attrs  # attributes get dropped, so we rem them
            da = da.weighted(measure.fillna(0)).mean(dim=measure.dims)
            da.attrs = attrs
            ds_sum[var] = da
        return xr.Dataset(ds_sum)

    if isinstance(dsd, xr.Dataset):
        return _global_mean(dsd)
    for key, ds in dsd.items():
        dsd[key] = _global_mean(ds)
    return dsd
