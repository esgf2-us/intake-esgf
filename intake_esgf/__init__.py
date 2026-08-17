# An intake and intake-esm inpsired catalog for ESGF

import warnings

import xarray as xr

warnings.simplefilter("ignore", category=xr.SerializationWarning)


def supported_projects() -> list[str]:
    """What projects are supported?"""
    from intake_esgf.projects import projects

    return sorted([p.__class__.__name__ for _, p in projects.items()])


from intake_esgf.catalog import ESGFCatalog  # noqa
from intake_esgf.config import conf  # noqa
from intake_esgf._version import __version__  # noqa

__all__ = ["ESGFCatalog", "conf", "supported_projects"]
