# An intake and intake-esm inpsired catalog for ESGF

import warnings

import xarray as xr

from intake_esgf.catalog import ESGFCatalog

warnings.simplefilter("ignore", category=xr.SerializationWarning)


__all__ = ["ESGFCatalog"]
