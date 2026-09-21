# An intake and intake-esm inpsired catalog for ESGF

import warnings

import xarray as xr

warnings.simplefilter("ignore", category=xr.SerializationWarning)


def supported_projects() -> list[str]:
    """What projects are supported?"""
    from intake_esgf.projects import projects

    # This is terrible, but a simple fix for the moment. Projects do not need to
    # be classes, they could just be dictionaries that can be read from a yaml
    # file so it is easier to expand.
    prjs = [
        p.replace("CORDEX", "CORDEX-")
        for p in sorted([p.__class__.__name__ for _, p in projects.items()])
    ]
    return prjs


from intake_esgf.catalog import ESGFCatalog  # noqa
from intake_esgf.config import conf  # noqa
from intake_esgf._version import __version__  # noqa

__all__ = ["ESGFCatalog", "conf", "supported_projects"]
