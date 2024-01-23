# An intake and intake-esm inpsired catalog for ESGF

import warnings

import xarray as xr
from pkg_resources import DistributionNotFound, get_distribution

warnings.simplefilter("ignore", category=xr.SerializationWarning)


def in_notebook() -> bool:
    """Check if the code is running in a jupyter notebook"""
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":  # Jupyter notebook, Spyder or qtconsole
            return True
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


IN_NOTEBOOK = in_notebook()

from intake_esgf.catalog import ESGFCatalog  # noqa

__all__ = ["ESGFCatalog", "IN_NOTEBOOK"]

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    __version__ = "0.0.0"  # pragma: no cover
